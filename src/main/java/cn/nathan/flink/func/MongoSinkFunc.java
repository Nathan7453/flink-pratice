package cn.nathan.flink.func;

import cn.nathan.flink.model.MongoDataItem;
import cn.nathan.flink.utils.JsonUtils;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import java.util.Stack;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSinkFunc extends RichSinkFunction<MongoDataItem> {

    private static String MONGO_URI = "mongodb://root:Dingchong%402019@10.82.0.93:27017/?authSource=admin";

    private static final Logger log = LoggerFactory.getLogger(MongoSinkFunc.class);

    public static MongoClient mongoClient() {
        return MongoClients.create(
            MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO_URI))
                .build());
    }

    private final Stack<MongoClient> connPolls = new Stack<>();

    @Override
    public void open(OpenContext openContext) throws Exception {
        try {
            for (int i = 0; i < 5; i++) {
                MongoClient client = mongoClient();
                connPolls.push(client);
            }
        } catch (Exception e) {
            log.error("创建mongo连接池已成: err = {}", e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        for (MongoClient client : connPolls) {
            try {
                client.close();
            } catch (Exception e) {
                // nothing to do
            }
        }
    }

    @Override
    public void invoke(MongoDataItem value, Context context) throws Exception {
        log.info("同步数据到mongo: {}", value);
        MongoClient client = null;
        try {
            client = connPolls.pop();
            MongoCollection<Document> collection = null;
            try {
                collection = client.getDatabase("test")
                    .getCollection("dev_elec");
            } catch (Exception e) {
                log.warn("忽略异常: err = {}", e.getMessage());
                client = mongoClient();
                collection = client.getDatabase("test")
                    .getCollection("dev_elec");
            }
            collection.insertOne(Document.parse(JsonUtils.toJsonString(value)));
        } catch (Exception e) {
            log.error("获取mongo客户端异常: err = {}", e.getMessage(), e);
        } finally {
            if (null != client) {
                connPolls.push(client);
            }
        }
    }
}
