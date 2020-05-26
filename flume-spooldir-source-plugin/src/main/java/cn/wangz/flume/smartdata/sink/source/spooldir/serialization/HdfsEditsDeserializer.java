package cn.wangz.flume.smartdata.sink.source.spooldir.serialization;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * @author wang_zh
 * @date 2020/5/25
 */
public class HdfsEditsDeserializer implements EventDeserializer {

    private final static Logger LOGGER = LoggerFactory.getLogger(HdfsEditsDeserializer.class);

    private static final Logger logger = LoggerFactory.getLogger(HdfsEditsDeserializer.class);
    private final ResettableInputStreamProxy in;
    private volatile boolean isOpen;
    private boolean skipBrokenEdits;

    private FSEditLogLoader.PositionTrackingInputStream tracker = null;
    private DataInputStream dataIn = null;
    private FSEditLogOp.Reader reader = null;
    private int maxOpSize = DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_DEFAULT;
    private int logVersion;

    public static final String SKIP_BROKEN_EDITS_KEY = "skipBrokenEdits";
    public static final boolean SKIP_BROKEN_EDITS_DEFAULT = false;

    public HdfsEditsDeserializer(Context context, ResettableInputStream in) throws IOException {
        this.in = new ResettableInputStreamProxy(in);
        this.skipBrokenEdits = context.getBoolean(SKIP_BROKEN_EDITS_KEY, SKIP_BROKEN_EDITS_DEFAULT);
        this.isOpen = true;

        tracker = new FSEditLogLoader.PositionTrackingInputStream(this.in);
        dataIn = new DataInputStream(tracker);

        logVersion = dataIn.readInt();
        if (NameNodeLayoutVersion.supports(
                LayoutVersion.Feature.ADD_LAYOUT_FLAGS, logVersion) ||
                logVersion < NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION) {
            try {
                LayoutFlags.read(dataIn);
            } catch (EOFException eofe) {
                throw new RuntimeException("EOF while reading layout flags from log");
            }
        }
        reader = FSEditLogOp.Reader.create(dataIn, tracker, logVersion);
        reader.setMaxOpSize(maxOpSize);
    }

    @Override
    public Event readEvent() throws IOException {
        FSEditLogOp editLogOp = nextOpImpl(false);
        if (editLogOp == null) {
            return null;
        }
        Event event = null;
        try {
            event = convertFSEditLogOp2Event(editLogOp);
        } catch (IllegalAccessException e) {
            LOGGER.warn("convert fs edit log to event error, {}", e);
        }
        LOGGER.info("event: {}", new String(event.getBody()));
        return event;
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        ensureOpen();
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent();
            if (event != null) {
                events.add(event);
            } else {
                break;
            }
        }
        return events;
    }

    private FSEditLogOp nextOpImpl(boolean skipBrokenEdits) throws IOException {
        return reader.readOp(skipBrokenEdits);
    }

    private Event convertFSEditLogOp2Event(FSEditLogOp editLogOp) throws IllegalAccessException {
        return FSEditLogOpConvert.convert(editLogOp);
    }


    @Override
    public void mark() throws IOException {
        ensureOpen();
        in.mark();
    }

    @Override
    public void reset() throws IOException {
        ensureOpen();
        in.reset();
    }


    @Override
    public void close() throws IOException {
        if (isOpen) {
            reset();
            in.close();
            isOpen = false;
        }
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw new IllegalStateException("Serializer has been closed");
        }
    }

    public static class Builder implements EventDeserializer.Builder {

        @Override
        public EventDeserializer build(Context context, ResettableInputStream in) {
            try {
                return new HdfsEditsDeserializer(context, in);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }
}
