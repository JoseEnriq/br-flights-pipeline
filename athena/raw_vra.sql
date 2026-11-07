CREATE EXTERNAL TABLE IF NOT EXISTS br_flights.raw_vra (
    airline_icao        string COMMENT 'ICAO Empresa Aérea',
    flight_number       string COMMENT 'Número Voo',
    authorization_code  string COMMENT 'Código Autorização (DI)',
    line_type           string COMMENT 'Código Tipo Linha',
    origin_icao         string COMMENT 'ICAO Aeródromo Origem',
    destination_icao    string COMMENT 'ICAO Aeródromo Destino',
    scheduled_departure string COMMENT 'Partida Prevista',
    actual_departure    string COMMENT 'Partida Real',
    scheduled_arrival   string COMMENT 'Chegada Prevista',
    actual_arrival      string COMMENT 'Chegada Real',
    flight_status       string COMMENT 'Situação Voo',
    justification_code  string COMMENT 'Código Justificativa'
)
COMMENT 'ANAC VRA files exactly as published, read in place on S3'
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
LOCATION 's3://${lake_bucket}/raw/vra/'
TBLPROPERTIES ('skip.header.line.count' = '2');
