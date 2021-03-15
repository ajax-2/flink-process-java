package flink.clickhouse;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MysqlToClickhouse
 * 原表: 192.168.1.63的trans_source.source3
 * 目标表: ck 192.168.0.201:9993 的default.sinkCk1
 * insert 增加模式
 */
public class MysqlToClickHouse {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance()
                .inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(env, streamSettings);
        tableEnvironment.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        // debezium 捕获到变化的数据会写入到这个 topic 中
        String topic1Name = "dbzium.debezium.mysqlsink";
        String topic2Name = "dbzium.debezium.sqlserversink";
        String bootKafkaServers = "192.168.0.71:9092";
        String group1ID = "flinkCk1";
        String group2ID = "flinkCk2";

        // 创建一个 Kafka 数据源的表
        tableEnvironment.executeSql("CREATE TABLE sourceCk1 (\n" +
                " id INT,\n" +
                " uid INT,\n" +
                " name STRING,\n" +
                " delete_flag INT,\n" +
                " update_flag INT,\n" +
                " insert_flag INT,\n" +
                " source_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,\n" +
                " age INT\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = '" + topic1Name + "',\n" +
                " 'properties.bootstrap.servers' = '" + bootKafkaServers + "',\n" +
                " 'debezium-json.schema-include' = 'true',\n" +
                " 'scan.startup.mode' = 'earliest-offset',\n" +
                " 'properties.group.id' = '" + group1ID + "',\n" +
                " 'format' = 'debezium-json'\n" +
                ")");

        tableEnvironment.executeSql("CREATE TABLE sourceCk2 (\n" +
                " id INT,\n" +
                " uid INT,\n" +
                " name STRING,\n" +
                " delete_flag INT,\n" +
                " update_flag INT,\n" +
                " insert_flag INT,\n" +
                " age INT\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = '" + topic2Name + "',\n" +
                " 'properties.bootstrap.servers' = '" + bootKafkaServers + "',\n" +
                " 'debezium-json.schema-include' = 'true',\n" +
                " 'scan.startup.mode' = 'earliest-offset',\n" +
                " 'properties.group.id' = '" + group2ID + "',\n" +
                " 'format' = 'debezium-json'\n" +
                ")");

        // 创建一个写入数据的 sink 表
        // 写入ck
        String url = "clickhouse://192.168.0.201:9993";
        String user = "default";
        String db = "default";
        String passwd = "";
        String tableName = "sinkCk3";
        tableEnvironment.executeSql("CREATE TABLE sinkCk3 (\n" +
                " id INT,\n" +
                " uid INT,\n" +
                " name STRING,\n" +
                " delete_flag INT,\n" +
                " update_flag INT,\n" +
                " insert_flag INT,\n" +
                " age INT,\n" +
                " source_time TIMESTAMP(3),\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'clickhouse',\n" +
                " 'url' = '" + url + "',\n" +
                " 'table-name' = '" + tableName + "',\n" +
                " 'username' = '" + user + "',\n" +
                " 'database-name' = '" + db + "',\n" +
                " 'password' = '" + passwd + "',\n" +
                " 'sink.batch-size' = '1000',\n" +
                " 'sink.flush-interval' = '1000',\n" +
                " 'sink.max-retries' = '3',\n" +
                " 'sink.partition-strategy' = 'hash',\n" +
                " 'sink.partition-key' = 'name'\n" +
                ")");
        // 更新配置
        String sql = "INSERT into sinkCk3(id, uid, name, delete_flag, update_flag, insert_flag, age, source_time) " +
                "select sourceCk1.id, sourceCk1.uid, CONCAT(sourceCk1.name,'&',sourceCk2.name),  " +
                "sourceCk1.delete_flag, sourceCk1.update_flag, sourceCk1.insert_flag, " +
                "sourceCk1.age, sourceCk1.source_time from sourceCk1 JOIN sourceCk2 on (sourceCk1.id = sourceCk2.id)";
        tableEnvironment.executeSql(sql);

        // 更新job名
        env.execute("sync-flink-ck-2");
    }
}
