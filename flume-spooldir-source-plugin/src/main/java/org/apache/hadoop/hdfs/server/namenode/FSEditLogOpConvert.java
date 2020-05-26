package org.apache.hadoop.hdfs.server.namenode;

import com.google.gson.Gson;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wang_zh
 * @date 2020/5/26
 */
public class FSEditLogOpConvert {

    private static final String DEFAULT_CHARSET = "utf-8";

    private static final Gson GSON = new Gson();

    public static Event convert(FSEditLogOp op) throws IllegalAccessException {
        Map<String, Object> map = new HashMap<>();
        FSEditLogOpCodes opCode = op.opCode;
        map.put("opCode", opCode.toString());
        long txid = op.txid;
        map.put("txid", txid);
        Field[] fields = op.getClass().getDeclaredFields();
        if (fields != null) {
            for (Field field: fields) {
                boolean changeAccess = false;
                if (Modifier.isPrivate(field.getModifiers())) {
                    field.setAccessible(true);
                    changeAccess = true;
                }
                String fieldName = field.getName();
                Object fieldValue = field.get(op);
                map.put(fieldName, fieldValue);
                if (changeAccess) {
                    field.setAccessible(false);
                }
            }
        }
        return EventBuilder.withBody(GSON.toJson(map), Charset.forName(DEFAULT_CHARSET));
    }

}
