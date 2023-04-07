CREATE EXTERNAL TABLE IF NOT EXISTS desafio_curso.TBL_ENDERECO ( 
Address_Number string,
City string,
Country string,
Customer_Address_1 string,
Customer_Adress_2 string,
Customer_Adress_3 string,
Costumes_Adress_4 string,
State string,
Zip_Code string
)
COMMENT 'Tabela de Enderecos'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';, \t\r\n'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '/datalake/raw/ENDERECO/'
TBLPROPERTIES ("skip.header.line.count"="1");