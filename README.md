### 版本
	flink: 1.12.0
        
        jdk: 1.8
 
        hive: 2.2.1

### flink-process-java
	flink 实时处理， 从debezium拿到数据，写入到clickhouse和mysql中

### 说明
	此项目仅有java文件， 暂不包含idea结构， 需要新建mvn项目， 进行代码更改

	hive CataLog: 需要将hive-site.xml（更改hive url, user, password） 移动到代码指定处，比如/tmp/hive-config/下

	此外， 所有的jar(包含hive catalog需要的包)包都添加到flink的lib目录下，这里不添加依赖包， jar包名为:

* commons-configuration2-2.7.jar
* commons-logging-1.2.jar
* flink-connector-clickhouse-1.11.0.jar
* flink-connector-jdbc_2.11-1.12.0.jar
* flink-connector-kafka_2.12-1.12.1.jar
* flink-csv-1.12.0.jar
* flink-dist_2.12-1.12.0.jar
* flink-json-1.12.0.jar
* flink-shaded-zookeeper-3.4.14.jar
* flink-sql-connector-hive-2.2.0_2.11-1.12.0.jar
* flink-table-api-java-bridge_2.11-1.12.0.jar
* flink-table-blink_2.12-1.12.0.jar
* flink-table-common-1.12.0.jar
* flink-table-planner-blink_2.11-1.12.0.jar
* flink-table_2.12-1.12.0.jar
* hadoop-auth-3.2.0.jar
* hadoop-client-3.2.0.jar
* hadoop-common-3.2.0.jar
* hadoop-core-1.2.1.jar
* hadoop-hdfs-client-3.2.0.jar
* hive-exec-2.1.1.jar
* htrace-core4-4.1.0-incubating.jar
* javax.servlet-api-3.1.0.jar
* kafka-clients-2.4.1.jar
* log4j-1.2-api-2.12.1.jar
* log4j-api-2.12.1.jar
* log4j-core-2.12.1.jar
* log4j-slf4j-impl-2.12.1.jar
* mysql-connector-java-5.1.48-sources.jar
* mysql-connector-java-5.1.48.jar
* postgresql-42.0.0.jar
* stax2-api-3.1.4.jar
* woodstox-core-5.0.3.jar	


