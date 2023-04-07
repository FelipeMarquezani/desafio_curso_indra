DADOS=("VENDAS" "CLIENTES" "DIVISAO" "ENDERECO" "REGIAO")

for i in "${DADOS[@]}"
do
	echo "$i"
    cd ../../raw/
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal /input/desafio_curso_indra/desafio_curso/raw/$i.csv /datalake/raw/$i
    beeline -u jdbc:hive2://localhost:10000/desafio_curso -f /input/desafio_curso_indra/desafio_curso/scripts/hql/create_table_$i.hql 
done
