#Importing libraries
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col

#Creating environment as streaming environment and creating a TableEnvironment
environment_settings = EnvironmentSettings.in_streaming_mode()
table_environment = TableEnvironment.create(environment_settings)

#Setting the jars to be used - here it is being used Apache Kafka, so there is a kafka connector
table_environment.get_config().get_configuration().set_string("pipeline.jars","file:///C:/Users/baral/Downloads/flink-sql-connector-kafka-3.0.1-1.18.jar")

#Creating the table that will be used as source
table_source = """CREATE TABLE table_source(chave VARCHAR, valorA VARCHAR, valorB INTEGER) WITH ('connector' = 'kafka','topic' = 'mensagens-produtor',
                                                                                'properties.bootstrap.servers' = '192.168.126.128:9092',
                                                                                'properties.group.id' = 'consumidores', 'scan.startup.mode' = 'earliest-offset',
                                                                                'format' = 'json')"""

#Creating the table that will be used as target (sink)
table_target = """CREATE TABLE table_target(valorA VARCHAR, valorB INTEGER, PRIMARY KEY (valorA) NOT ENFORCED) WITH ('connector' = 'upsert-kafka','topic' = 'mensagens-consumidor',
                                                                                                        'properties.bootstrap.servers' = '192.168.126.128:9092',
                                                                                                        'key.format' = 'json','value.format' = 'json')"""

#Running the sql statements to create source and target tables
table_environment.execute_sql(table_source)
table_environment.execute_sql(table_target)

#Creating sql statement that selects data from source
statement = "SELECT valorA,valorB FROM table_source"

#Running sql statementExecute the query and retrieve the result table
result_statement = table_environment.sql_query(statement).group_by(col('valorA')).select(col('valorA'),col('valorB').sum)

#Inserting data to the target table
result_statement.execute_insert('table_target').wait()

