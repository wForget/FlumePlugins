package cn.wangz.flume.smartdata.sink;

import com.google.gson.Gson;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.LoggerSink;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.client.SmartClient;
import org.smartdata.conf.SmartConfKeys;
import org.smartdata.metrics.FileAccessEvent;

import java.io.IOException;
import java.util.Map;

/**
 * @author wang_zh
 * @date 2020/5/26
 */
public class SmartdataSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory
            .getLogger(LoggerSink.class);

    private SmartClient smartClient = null;

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;

        try {
            transaction.begin();
            event = channel.take();

            if (event != null) {
                processEvent(event);
            } else {
                // No event found, request back-off semantics from the sink runner
                result = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Exception ex) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to log event: " + event, ex);
        } finally {
            transaction.close();
        }

        return result;
    }

    private static final Gson GSON = new Gson();

    public void processEvent(Event event) {
        try {
            String json = new String(event.getBody());
            Map<String, Object> map = GSON.fromJson(json, Map.class);
            String opCode = String.valueOf(map.get("opCode"));
            switch (opCode) {
                case "OP_TIMES":
                    Object atime = map.get("atime");
                    if (atime == null) {
                        break;
                    }
                    this.smartClient.reportFileAccessEvent(new FileAccessEvent(String.valueOf(map.get("path")), (Long) atime,null));
                    break;
                default:
                    break;
            }
        } catch (Throwable t) {
            logger.warn("smartdata sink process event fail, error: {}", t);
        }
    }

    private static final String SMART_SERVER_RPC_ADDRESS_KEY = "smartAddress";
    private static final String SMART_IGNORE_DIRS_KEY = "ignoreDirs";
    private static final String SMART_WORK_DIR_KEY = "workDir";
    private static final String SMART_COVER_DIRS_KEY = "coverDirs";
    @Override
    public void configure(Context context) {
        Configuration configuration = new Configuration();
        if (context.containsKey(SMART_SERVER_RPC_ADDRESS_KEY)) {
            configuration.set(SmartConfKeys.SMART_SERVER_RPC_ADDRESS_KEY, context.getString(SMART_SERVER_RPC_ADDRESS_KEY));
        }
        if (context.containsKey(SMART_IGNORE_DIRS_KEY)) {
            configuration.set(SmartConfKeys.SMART_IGNORE_DIRS_KEY, context.getString(SMART_IGNORE_DIRS_KEY));
        }
        if (context.containsKey(SMART_WORK_DIR_KEY)) {
            configuration.set(SmartConfKeys.SMART_WORK_DIR_KEY, context.getString(SMART_WORK_DIR_KEY));
        }
        if (context.containsKey(SMART_COVER_DIRS_KEY)) {
            configuration.set(SmartConfKeys.SMART_COVER_DIRS_KEY, context.getString(SMART_COVER_DIRS_KEY));
        }
        try {
            this.smartClient = new SmartClient(configuration);
        } catch (IOException e) {
            throw new FlumeException("init smart client fail, error: {}", e);
        }
    }
}
