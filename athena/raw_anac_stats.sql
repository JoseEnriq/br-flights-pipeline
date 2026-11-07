CREATE EXTERNAL TABLE IF NOT EXISTS br_flights.raw_anac_stats (
    airline_icao                string COMMENT 'EMPRESA_SIGLA',
    airline_name                string COMMENT 'EMPRESA_NOME',
    airline_nationality         string COMMENT 'EMPRESA_NACIONALIDADE',
    report_year                 string COMMENT 'ANO',
    report_month                string COMMENT 'MES',
    origin_icao                 string COMMENT 'AEROPORTO_DE_ORIGEM_SIGLA',
    origin_city                 string COMMENT 'AEROPORTO_DE_ORIGEM_NOME',
    origin_state                string COMMENT 'AEROPORTO_DE_ORIGEM_UF',
    origin_region               string COMMENT 'AEROPORTO_DE_ORIGEM_REGIAO',
    origin_country              string COMMENT 'AEROPORTO_DE_ORIGEM_PAIS',
    origin_continent            string COMMENT 'AEROPORTO_DE_ORIGEM_CONTINENTE',
    destination_icao            string COMMENT 'AEROPORTO_DE_DESTINO_SIGLA',
    destination_city            string COMMENT 'AEROPORTO_DE_DESTINO_NOME',
    destination_state           string COMMENT 'AEROPORTO_DE_DESTINO_UF',
    destination_region          string COMMENT 'AEROPORTO_DE_DESTINO_REGIAO',
    destination_country         string COMMENT 'AEROPORTO_DE_DESTINO_PAIS',
    destination_continent       string COMMENT 'AEROPORTO_DE_DESTINO_CONTINENTE',
    flight_nature               string COMMENT 'NATUREZA',
    flight_group                string COMMENT 'GRUPO_DE_VOO',
    paid_passengers             string COMMENT 'PASSAGEIROS_PAGOS',
    free_passengers             string COMMENT 'PASSAGEIROS_GRATIS',
    paid_cargo_kg               string COMMENT 'CARGA_PAGA_KG',
    free_cargo_kg               string COMMENT 'CARGA_GRATIS_KG',
    mail_kg                     string COMMENT 'CORREIO_KG',
    ask                         string COMMENT 'ASK',
    rpk                         string COMMENT 'RPK',
    atk                         string COMMENT 'ATK',
    rtk                         string COMMENT 'RTK',
    fuel_liters                 string COMMENT 'COMBUSTIVEL_LITROS',
    distance_flown_km           string COMMENT 'DISTANCIA_VOADA_KM',
    departures                  string COMMENT 'DECOLAGENS',
    paid_cargo_km               string COMMENT 'CARGA_PAGA_KM',
    free_cargo_km               string COMMENT 'CARGA_GRATIS_KM',
    mail_km                     string COMMENT 'CORREIO_KM',
    seats                       string COMMENT 'ASSENTOS',
    payload                     string COMMENT 'PAYLOAD',
    hours_flown                 string COMMENT 'HORAS_VOADAS',
    baggage_kg                  string COMMENT 'BAGAGEM_KG'
)
COMMENT 'ANAC air transport statistics file exactly as published, read in place on S3'
PARTITIONED BY (
    ingestion_date string COMMENT 'Day this version was loaded, YYYY-MM-DD'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = '\;',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
LOCATION 's3://${lake_bucket}/raw/anac_stats/'
TBLPROPERTIES ('skip.header.line.count' = '2');
