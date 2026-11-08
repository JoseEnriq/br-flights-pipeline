# br-flights-pipeline

[![ci](https://github.com/JoseEnriq/br-flights-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/JoseEnriq/br-flights-pipeline/actions/workflows/ci.yml)

[English](README.md) | **Português**

Um pipeline diário na AWS com todos os voos regulares do Brasil, a partir dos dados abertos da ANAC. Ele percebe quando a ANAC publica ou republica em silêncio um mês, guarda cada versão que já viu, reconstrói só o que mudou e confere os próprios números com os percentuais de atraso que a ANAC publica.

## O problema

A ANAC publica um arquivo de voos por mês, cerca de quatro semanas depois do fim do mês, e regera os arquivos recentes todo dia, às vezes mudando voos, quase sempre não. Meses antigos também mudam: janeiro de 2019 foi regerado em maio de 2026. Um projeto que carrega uma pasta de arquivos uma vez nunca vê nada disso. Os arquivos ainda têm armadilhas: uma linha de metadado antes do cabeçalho, dois meses exportados com todas as datas trocadas pelo texto `Microsoft.SqlServer.Dts.Pipeline.BlobColumn` e percentuais de atraso que contam cada número de voo de codeshare como um voo separado.

## Como funciona

```
ANAC ──► ingest (hash dos voos, uma pasta no S3 por versão real) ──► raw em CSV
                                                                       │  Glue + Athena
         dbt: staging em Parquet, incremental por mês ◄────────────────┘
          └► fato de voos, dimensões de companhia, aeroporto e data, pontualidade, conciliação com a ANAC

O Airflow (Docker) roda tudo todo dia às 20h e manda e-mail pelo SNS quando uma tarefa falha.
O Terraform guarda os recursos da AWS; o GitHub Actions roda testes e checagens estáticas.
```

Um dia sem novidade termina nos downloads, sem consulta no Athena. Quando um arquivo muda, a partição dele é registrada e o dbt reconstrói as camadas acima.

## Principais decisões

Todas, com as alternativas que descartei, estão em [docs/decisions.md](docs/decisions.md).

- **A mudança é detectada pelo conteúdo, não pela data.** O SHA-256 ignora a linha `Atualizado em` que a ANAC regrava todo dia, então só uma mudança real gera versão nova.
- **dbt no Athena, e não Spark.** Uns 12 MB de CSV por mês não justificam um cluster, e o Athena cobra por bytes lidos.
- **Meses corrompidos ficam na raw e fora do modelo**, listados à mão no `dbt_project.yml`, para que um erro novo do mesmo tipo quebre o build em vez de ser filtrado em silêncio.
- **Um voo por linha.** Quando um número de voo opera duas vezes perto da mesma data, a ANAC cruza cada horário previsto com cada real; cada voo fica com o horário real mais próximo do previsto.
- **Um usuário IAM com permissão mínima roda o pipeline**; nenhuma credencial da AWS fica no GitHub.

## O que os dados mostram

Das consultas em [athena/queries/](athena/queries/) e dos modelos em [dbt/](dbt/):

- **A pandemia:** 6.227 voos realizados em abril de 2020, contra 75.574 em abril de 2019 (−92%). Só abril de 2024 passou o nível de 2019.
- **O modelo bate com a ANAC:** nos 24 meses em que os percentuais da própria ANAC são uma referência confiável, de 95,0% a 99,7% das combinações de companhia, número de voo e rota batem com as contagens e taxas publicadas.
- **Os percentuais da ANAC contam codeshares:** agosto de 2026 lista 208.412 etapas previstas para 85.713 voos previstos. Somar esses percentuais para contar voos conta o mesmo avião várias vezes.
- **O Parquet compensa:** 1,08 GB de CSV na raw desde 2019 viraram 92 MB de Parquet no staging.
- **Custa centavos:** US$ 0,31 de uso da AWS do início do projeto até 8 de novembro de 2026, com o histórico desde 2019.

## O que ficou de fora

Streaming, porque a ANAC publica arquivos, e não eventos; Spark e um data warehouse, que esse volume não pede. As versões antigas da raw não podem ser recriadas por ninguém: a ANAC só oferece o arquivo atual, então elas só existem no bucket deste projeto.

## Como reproduzir

Requer Python 3.12, Terraform 1.10+, uma conta AWS e, para a execução diária, Docker. Copie o `.env.example` para `.env` e defina em `LAKE_BUCKET` um nome de bucket seu, que o `infra/` também precisa (`lake_bucket` e o bloco do backend).

```bash
pip install -r requirements-dev.txt
cd infra && terraform init && terraform apply && cd ..   # numa conta nova, começa com estado local
python pipeline/create_raw_tables.py
python pipeline/ingest.py --since 2019-01 && python pipeline/ingest_punctuality.py --since 2019-01 && python pipeline/ingest_stats.py
cd dbt && dotenv -f ../.env run -- dbt build && cd ..
cd airflow && docker compose up -d --build                # Airflow em http://localhost:8080
```

O Airflow também precisa de uma chave de acesso do usuário do pipeline, num perfil da AWS chamado `br-flights-pipeline`, criada no console, fora do Terraform. O primeiro `dbt build` sobre o histórico inteiro passa do limite de 1 GB do workgroup; o [docs/decisions.md](docs/decisions.md) mostra a carga em lotes. Dados: [dados abertos da ANAC](https://sistemas.anac.gov.br/dadosabertos/), não redistribuídos aqui.
