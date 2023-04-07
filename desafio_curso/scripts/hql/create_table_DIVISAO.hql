CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_DIVISAO (
Division int,
Division_Name string
)
COMMENT 'Tabela de Divisao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '/datalake/raw/DIVISAO/'
TBLPROPERTIES ("skip.header.line.count"="1");