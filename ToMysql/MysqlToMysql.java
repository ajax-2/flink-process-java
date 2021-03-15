package flink.mysql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MysqlToMysql
 * 从mysql 到 mysql
 * 原表为 192.168.1.63的trans_source.source1
 * 目标表为 192.168.0.201:3307的debezium.mysqlsink
 * 镜像复制
 */
public class MysqlToMysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, bsSettings);
        // debezium 捕获到变化的数据会写入到这个 topic 中
        String topicName = "demo.flink.mysql.trans_source.source1";
        String bootKafkaServers = "192.168.0.71:9092";
        String groupID = "mysqlsinksource";

        String sinkJdbcUrl = "jdbc:mysql://192.168.1.88:3306/debezium";
        String sinkUser = "debezium";
        String sinkPassword = "debezium";
        String sinkTable = "mysqlsink";

        // 创建一个 Kafka 数据源的表
        tableEnvironment.executeSql("CREATE TABLE demoSre (\n" +
                " id INT,\n" +
                " uid INT,\n" +
                " name STRING,\n" +
                " age INT\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = '" + topicName + "',\n" +
                " 'properties.bootstrap.servers' = '" + bootKafkaServers + "',\n" +
                " 'debezium-json.schema-include' = 'true',\n" +
                " 'scan.startup.mode' = 'earliest-offset',\n" +
                " 'properties.group.id' = '" + groupID + "',\n" +
                " 'format' = 'debezium-json'\n" +
                ")");

        // 创建一个写入数据的 sink 表
        tableEnvironment.executeSql("CREATE TABLE demoSk (\n" +
                " id INT,\n" +
                " uid INT,\n" +
                " name STRING,\n" +
                " age INT,\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = '" + sinkJdbcUrl + "',\n" +
                " 'username' = '" + sinkUser + "',\n" +
                " 'password' = '" + sinkPassword + "',\n" +
                " 'table-name' = '" + sinkTable + "'\n" +
                ")");
        String updateSQL = "insert into demoSk(id, uid, name, age) select id, uid, name, age from demoSre";
        tableEnvironment.executeSql(updateSQL);
        env.execute("demoHueFlink");
    }
}
