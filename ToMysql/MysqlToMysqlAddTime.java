package flink.mysql;


import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MysqlToMysql
 * 从mysql 到 mysql
 * 原表为 192.168.1.63的trans_source.source3
 * 目标表为 192.168.1.63的trans_sink/sink-mysql3
 * 目标表中新增binlog生成的时间
 */
public class MysqlToMysqlAddTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 测试checkpoint
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 添加checkpoint结束

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, bsSettings);
        // debezium 捕获到变化的数据会写入到这个 topic 中
        String topicName = "demo.flink.mysql.trans_source.source3";
        String bootKafkaServers = "192.168.0.71:9092";
        String groupID = "flinkGroup3";

        String sinkJdbcUrl = "jdbc:mysql://192.168.1.63:3306/trans_sink";
        String sinkUser = "root";
        String sinkPassword = "abc123";
        // 更新sinkTable为sink-mysql2
        String sinkTable = "sink-mysql3";

        // 创建一个 Kafka 数据源的表
        // 更新sourceFlink -> sourceFlink2, 更新topic 为source2, 更改group.id 为group2
        tableEnvironment.executeSql("CREATE TABLE sourceFlink4 (\n" +
                " topic_timestamp TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,\n" +
                " id INT,\n" +
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
        // 更新sinkFlink -> sinkFlink2
        tableEnvironment.executeSql("CREATE TABLE sinkFlink4 (\n" +
                " topic_timestamp TIMESTAMP(3),\n" +
                " id INT,\n" +
                " name STRING,\n" +
                " age INT,\n" +
                " PRIMARY KEY (topic_timestamp, id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = '" + sinkJdbcUrl + "',\n" +
                " 'username' = '" + sinkUser + "',\n" +
                " 'password' = '" + sinkPassword + "',\n" +
                " 'table-name' = '" + sinkTable + "'\n" +
                ")");
        // 更新配置
        String updateSQL = "insert into sinkFlink4 select * from sourceFlink4";
        tableEnvironment.executeSql(updateSQL);

        // 更新job名
        env.execute("sync-flink-cdc4");
    }
}
