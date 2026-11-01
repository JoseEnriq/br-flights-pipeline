# Decisões

Cada decisão do projeto, com o motivo e a alternativa descartada. Organizado por camada do pipeline.

## Fonte

- **VRA (Voo Regular Ativo) da ANAC.** Todos os voos regulares do Brasil, um CSV por mês desde 2000, publicado cerca de quatro semanas depois do fim do mês. Foi escolhido por ser uma fonte viva: sai um mês novo todo mês e os meses recentes são regerados todo dia, o que exige carga incremental de verdade. Alternativas comparadas: cota parlamentar da Câmara (modelagem mais rica, mas com CPF de fornecedores e arquivos anuais), preços da ANP (nomes de arquivo inconsistentes), CNPJ da Receita (fotos completas pesadas, sem incremental) e NYC Taxi (muito usado em portfólio).
- **Sem dicionário de dados oficial.** A ANAC não publica o significado dos códigos de tipo de linha e de autorização. As suposições estão nas descrições das colunas em `dbt/models/staging/_stg_models.yml`.
- **Os dados não são publicados.** O bucket é privado e nenhum arquivo da ANAC entra no Git; o repositório guarda o código e números agregados, sempre citando a fonte.

## Ingestão (raw)

- **ELT: a raw guarda o arquivo como veio**, com BOM, linha de metadado e nulos escritos como texto. A limpeza fica no dbt, e um erro de transformação se corrige refazendo a partir da raw.
- **Mudança detectada por conteúdo, não por data.** A ANAC regrava a linha `Atualizado em: <data>` todo dia, mesmo sem mudar nenhum voo. O hash SHA-256 ignora essa linha, então só uma mudança real gera versão nova.
- **Uma pasta por versão** (`raw/vra/month=AAAA-MM/ingestion_date=AAAA-MM-DD/`), sem sobrescrever e sem versionamento do S3. Assim dá para comparar duas versões do mesmo mês no Athena.
- **Partição pelo mês do arquivo publicado**, não pela data do voo: cada arquivo traz alguns voos dos primeiros dias do mês seguinte, mas os meses não se sobrepõem.
- **Estado em JSON no próprio bucket** (`_state/vra_ingest.json`), gravado depois de cada arquivo. DynamoDB foi descartado: são algumas dezenas de meses e um único processo escrevendo.
- **Janela de conferência de 13 meses** (o mês atual e os 12 anteriores) em toda execução, e o histórico inteiro com `--since`. Baixar todos os meses todo dia seria desperdício, porque os antigos quase nunca mudam.

## Catálogo da raw (Athena)

- **Tabela declarada à mão, sem Glue Crawler.** O crawler adivinha o formato olhando os arquivos e escreve a tabela no catálogo sozinho; não move nem transforma dados, só substitui o `CREATE TABLE` e o `MSCK REPAIR`. Num arquivo fora do padrão como o da ANAC (BOM, linha de metadado antes do cabeçalho, separador `;`), ele erra, e ainda alteraria a tabela sem revisão. A tabela escrita à mão é exata, gratuita e versionada. Detalhes em `athena/README.md`.
- **Experimento que confirmou a decisão** (2026-10-30, banco de teste apagado depois): dois crawlers com a mesma role, um sobre setembro de 2026 na raw e outro sobre o staging em Parquet.

  | | Crawler no CSV da raw | Crawler no Parquet do staging |
  |---|---|---|
  | Formato reconhecido | `UNKNOWN` | `parquet` |
  | Colunas | nenhuma | as 13, com as datas como `timestamp` |
  | Consulta no Athena | falha com `HIVE_UNSUPPORTED_FORMAT` | igual à tabela do dbt (1.021.748 linhas) |

  O resultado no CSV foi pior do que o esperado: em vez de colunas erradas, nenhuma. O classificador de CSV do Glue até testa o `;`, mas exige um arquivo com pelo menos duas colunas em todas as linhas, e a primeira linha da ANAC (`Atualizado em: ...`) tem uma só; a causa mais provável é essa, somada ao BOM. Um classificador personalizado define separador e cabeçalho, mas não pula a linha de metadado. O Parquet traz o esquema dentro do arquivo, então o crawler só o lê. Ele é uma boa escolha para formatos assim, para CSV e JSON bem comportados e para lakes com muitas tabelas ou esquemas que mudam com frequência; não para arquivos fora do padrão.
- **Todas as colunas como texto na raw.** O formato das datas e dos nulos muda entre épocas da fonte; os tipos ficam no staging, onde cada regra é testada.
- **Colunas em inglês, valores como vieram.** Nomes sem acento nem espaço evitam aspas em todo SQL; o nome original da ANAC fica no comentário de cada coluna. Situação, aeroportos e companhias continuam como a ANAC publica.
- **Workgroup `br-flights` com limite de 1 GB por consulta** e resultados em `_athena-results/`, apagados em 7 dias por uma regra de ciclo de vida. Resultados e dados no mesmo bucket, com SSE-S3, é uma simplificação: empresas costumam separar o bucket de resultados e usar SSE-KMS.
- **O workgroup não força as configurações do cliente.** Com *Override client-side settings* ligado, o Athena ignora o `external_location` dos modelos do dbt e grava as tabelas dentro de `_athena-results/`, onde elas seriam apagadas em 7 dias. Isso aconteceu na primeira execução do staging e foi corrigido desligando a opção.

## Staging (dbt + Athena)

- **dbt Core v1 com o adaptador `dbt-athena`.** O dbt v2 (2.0.9) recusa o Athena: `unknown variant athena, expected one of redshift, snowflake, postgres, bigquery, trino, ...`. O adaptador `trino` não serve, porque o Athena só é acessado pela API da AWS.
- **Python 3.12.** O `dbt-athena` 1.11.1 exige `mmh3<4.2`, e o `mmh3` 4.1.0 não tem pacote pronto para Windows no Python 3.13; a instalação pediria um compilador C++. O `dbt-core` fica fixado na versão estável (1.12.6): sem isso, o pip escolhe uma beta, porque o `dbt-athena` aceita versões candidatas.
- **Parquet com compressão ZSTD.** O Athena cobra pelos bytes lidos já comprimidos; o ZSTD comprime mais que o Snappy, que é o padrão mais comum, com leitura igualmente rápida.
- **Tabela Hive incremental com `insert_overwrite`, particionada por mês.** A unidade de mudança da fonte é o mês inteiro, e essa estratégia substitui a partição do mês sem misturar versões. Iceberg foi descartado por enquanto: no `dbt-athena`, ele só aceita `append` e `merge`, que pedem uma chave única que a fonte não garante.
- **Só a versão mais recente de cada mês, e só quando ela ainda não está no staging.** As versões a processar vêm do catálogo (`raw_vra$partitions`, que não lê dados) e entram no SQL como filtros literais, porque o Athena não poda partições a partir de uma subconsulta. Quando nada mudou, o filtro vira `false`: a segunda execução seguida leu 1,7 KB de metadados e não gravou nada.
- **`cast` em vez de `try_cast` nas datas.** Uma data inválida deve quebrar o modelo, não virar nulo em silêncio. Fevereiro e março de 2020 estão corrompidos na fonte e vão precisar de tratamento explícito quando o histórico for carregado.
- **Linhas repetidas por inteiro saem no staging** (`select distinct`): 18 em 1.021.766 voos nos 12 primeiros meses.
- **A chave do voo é testada com aviso, não com erro.** Companhia, número, origem e partida prevista (ou a real, para voos fora da grade) se repetem em 102 casos com horários reais diferentes, como o ACA 0090 de 27/10/2025, registrado saindo à 01:29 e às 23:08. Escolher a linha certa é regra de negócio e fica para o modelo de voos.
- **Voos com origem igual ao destino** (4.133, todos realizados) ficam no staging; o significado é decidido no modelo de voos.

### CSV × Parquet

Os mesmos 12 meses (outubro de 2025 a setembro de 2026), medidos em 2026-10-30 com as consultas de `athena/queries/csv_vs_parquet.sql`:

| | Raw (CSV) | Staging (Parquet) |
|---|---|---|
| Tamanho no S3 | 144,8 MB | 13,2 MB |
| Situação dos voos de setembro de 2026 | 12,1 MB lidos | 3,7 KB lidos |
| Voos que decolaram, todos os meses | 144,8 MB lidos | 5,0 MB lidos |

O Parquet lê só as colunas da consulta, comprime cada coluna separadamente (a situação do voo tem dois valores em 85 mil linhas) e guarda as datas como números em vez de texto.

## Modelo de voos (curated)

- **Uma linha por voo.** O voo é a companhia, o número, a origem e a partida prevista. Voos fora da grade não têm partida prevista e usam a real, num espaço de chave separado: a primeira versão da chave misturava os dois e acusava como repetidos registros diferentes, como o AFR 0460 de 27/11/2025, previsto de Paris para Guarulhos e cancelado, e o mesmo número saindo no mesmo minuto para Lisboa, fora da grade. Com a origem e o espaço separado, as chaves repetidas caíram de 122 para 102.
- **Nas chaves repetidas, fica o horário real mais próximo do previsto.** Quando o mesmo número de voo opera duas vezes perto da mesma data, a ANAC combina cada horário previsto com cada horário real. O AAL 0995 de 21/05/2026, previsto às 00:00 e às 23:50 e realizado às 23:55 do dia 20 e às 23:50 do dia 21, aparece em quatro linhas, duas delas com quase 24 horas de "atraso". Manter tudo inflaria a contagem e a média de atraso; apagar as chaves repetidas perderia também os voos certos. O custo: quando a ANAC mostra um só horário previsto para duas operações, a segunda sai do fato. Ficaram 1.021.646 voos.
- **`flight_id` é um hash da chave**, não um número sequencial: o mesmo voo recebe o mesmo identificador em toda reconstrução. É um `xxhash64` guardado como número de 64 bits; a primeira versão, um MD5 em texto hexadecimal, deixava a tabela com 36,6 MB em vez de 25,7 MB, porque texto aleatório quase não comprime.
- **Voos com origem igual ao destino são retornos.** Os 4.133 estão fora da grade e foram realizados; 3.859 têm código de autorização 3, e todo voo com código 3 volta à origem. O SID 9233 de 30/11/2025 saiu de Fortaleza à 01:45, voltou às 02:15 (código 3) e saiu de novo às 03:50 para Confins (código 7). O significado do código vem dos dados: o dicionário do VRA não foi encontrado no site da ANAC. Os retornos ficam no fato como operações (`is_return_to_origin`) e não entram nas métricas de rota nem de pontualidade, porque não têm horário previsto.
- **Atraso é a definição da ANAC:** chegada real mais de 30 (ou 60) minutos depois da prevista, contada só para voos previstos que foram realizados; o cancelamento é contado sobre os voos previstos. A regra foi descoberta reproduzindo os *Percentuais de atrasos e cancelamentos* publicados pela ANAC: em três rotas de agosto de 2026 (AAL 0963 Dallas–Guarulhos, Azul Confins–Guarulhos e Curitiba–Guarulhos), a chegada bate com os números oficiais e a partida não. Os flags ficam vazios fora desse escopo, para que a média de um flag seja a própria taxa.
- **Atrasos de mais de um dia ficam como publicados**, porque a ANAC os conta nos próprios percentuais, mas um teste com aviso os lista: 240 chegadas mais de um dia atrasadas e 22 mais de um dia adiantadas, que só podem ser erro.
- **Fato reconstruído inteiro a cada execução**, sem incremental. A entrada já é Parquet: o histórico todo custa cerca de 13 MB de leitura. O incremental fica onde ele economiza, no download e na conversão do CSV.
- **Horários sem conversão de fuso.** Previsto e real estão na mesma referência, então o atraso não depende do fuso; a duração dos voos internacionais sugere um fuso único para todos os aeroportos, ainda não confirmado.
- **Calendário de 2019 a 2030**, gerado no Athena. O `sequence()` aceita até 10.000 elementos, cerca de 27 anos de dias.
