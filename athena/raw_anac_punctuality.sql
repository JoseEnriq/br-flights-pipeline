CREATE EXTERNAL TABLE IF NOT EXISTS br_flights.raw_anac_punctuality (
    airline           string COMMENT 'Empresa_Aerea',
    flight_number     string COMMENT 'N_Voo',
    origin_icao       string COMMENT 'Aeroporto_Origem_Designador_OACI',
    origin_name       string COMMENT 'Aeroporto_Origem_Nome_UF_Pais',
    destination_icao  string COMMENT 'Aeroporto_Destino_Designador_OACI',
    destination_name  string COMMENT 'Aeroporto_Destino_Nome_UF_Pais',
    scheduled_legs    string COMMENT 'Etapas_Previstas',
    pct_cancelled     string COMMENT 'Percentuais_de_Cancelamentos',
    pct_delayed_30    string COMMENT 'Percentuais_de_Atrasos_superiores_a_30_minutos',
    pct_delayed_60    string COMMENT 'Percentuais_de_Atrasos_superiores_a_60_minutos'
)
COMMENT 'ANAC monthly delay and cancellation percentages (Annex I) exactly as published, read in place on S3'
PARTITIONED BY (
    `month`        string COMMENT 'Month of the published file, YYYY-MM',
    ingestion_date string COMMENT 'Day this version was loaded, YYYY-MM-DD'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = '\;',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
LOCATION 's3://${lake_bucket}/raw/anac_punctuality/'
TBLPROPERTIES ('skip.header.line.count' = '2');
