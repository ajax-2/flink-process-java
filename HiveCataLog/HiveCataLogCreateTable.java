package flink.kafka.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Consume2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        // hive catalog
        String name = "dbz";

        Catalog catalog = new HiveCatalog(name, null, "/tmp/hive-config");
        bsTableEnv.registerCatalog(name, catalog);
        bsTableEnv.useCatalog(name);

        // table
        String topicName = "demo.flink.mysql.trans_source.source3";
        String bootKafkaServers = "192.168.0.71:9092";
        String groupID = "TestCatalog";
        String kafka_sql = "CREATE TABLE table1 (\n" +
                " id INT,\n" +
                " age INT,\n" +
                " name STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = '" + topicName + "',\n" +
                " 'properties.bootstrap.servers' = '" + bootKafkaServers + "',\n" +
                " 'debezium-json.schema-include' = 'true',\n" +
                " 'scan.startup.mode' = 'earliest-offset',\n" +
                " 'properties.group.id' = '" + groupID + "',\n" +
                " 'format' = 'debezium-json'\n" +
                ")";
        bsTableEnv.executeSql(kafka_sql);

        Table table1 = bsTableEnv.sqlQuery("select * from table1");
        table1.execute().print();

        bsEnv.execute("catalog");
    }

}
