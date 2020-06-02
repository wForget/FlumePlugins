package cn.wangz.flume.channel.memory.high;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author wang_zh
 * @date 2020/5/29
 */
public class HighMemoryChannel extends MemoryChannel {

    private static Logger LOGGER = LoggerFactory.getLogger(HighMemoryChannel.class);

    private String stopFilePath = null;

    @Override
    public void configure(Context context) {
        this.stopFilePath = context.getString("stopFile", "._stopFile");
        super.configure(context);
    }

    @Override
    public synchronized void start() {
        try {
            Field field = MemoryChannel.class.getDeclaredField("queue");
            field.setAccessible(true);
            LinkedBlockingDeque<Event> queue = (LinkedBlockingDeque<Event>) field.get(this);
            readStopFile(queue);
            field.setAccessible(false);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOGGER.warn("HighMemoryChannel stop problems, msg: {}", e);
        }

        super.start();
    }

    @Override
    public synchronized void stop() {
        super.stop();

        try {
            Field field = getClass().getDeclaredField("queue");
            field.setAccessible(true);
            LinkedBlockingDeque<Event> queue = (LinkedBlockingDeque<Event>) field.get(this);
            writeStopFile(queue);
            field.setAccessible(false);
        } catch (NoSuchFieldException | IllegalAccessException | IOException e) {
            LOGGER.warn("HighMemoryChannel stop problems, msg: {}", e);
        }
    }

    private void readStopFile(LinkedBlockingDeque<Event> queue) {
        File file = new File(this.stopFilePath);
        if (!file.exists()) {
            return;
        }
        int count = 0;
        try {
            EventDeserializer.Builder builder = new AvroEventDeserializer.Builder();
            ResettableInputStream in =
                    new ResettableFileInputStream(file, new PositionTracker() {
                        @Override
                        public void storePosition(long position) throws IOException {
                        }

                        @Override
                        public long getPosition() {
                            return 0;
                        }

                        @Override
                        public String getTarget() {
                            return null;
                        }

                        @Override
                        public void close() throws IOException {
                        }
                    }, ResettableFileInputStream.DEFAULT_BUF_SIZE, Charset.forName("utf-8"), DecodeErrorPolicy.FAIL);
            EventDeserializer deserializer = builder.build(new Context(), in);
            try {
                List<Event> events = deserializer.readEvents(queue.size());
                queue.addAll(events);
                count = queue.size();
            } catch (IOException e) {
                LOGGER.warn("HighMemoryChannel writeStopFile serializer fail, msg: {}", e);
            }
        } catch (Throwable t){
            LOGGER.warn("HighMemoryChannel readStopFile fail, msg: {}", t);
        }
        LOGGER.info("HighMemoryChannel read stop file success, event count: {}", count);
    }

    private void writeStopFile(LinkedBlockingDeque<Event> queue) throws IOException {
        if (queue.size() < 1) {
            return;
        }
        LOGGER.info("HighMemoryChannel write stop file prepare, event count: {}", queue.size());
        File file = new File(this.stopFilePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        LongAdder count = new LongAdder();
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            EventSerializer.Builder builder = new FlumeEventAvroEventSerializer.Builder();
            EventSerializer eventSerializer = builder.build(new Context(), fileOutputStream);
            queue.forEach(event -> {
                try {
                    eventSerializer.write(event);
                    count.increment();
                } catch (IOException e) {
                    LOGGER.warn("HighMemoryChannel writeStopFile serializer fail, msg: {}", e);
                }
            });
            eventSerializer.flush();
        } catch (Throwable t){
            LOGGER.warn("HighMemoryChannel writeStopFile fail, msg: {}", t);
        }
        LOGGER.info("HighMemoryChannel write stop file success, event count: {}", count.intValue());
    }
}
