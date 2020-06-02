package cn.wangz.flume.channel.memory.high;

import javax.annotation.concurrent.GuardedBy;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author wang_zh
 * @date 2020/5/29
 */
public class QueueAccessTest {

    public static void main(String[] args) {
        Demo2 demo2 = new Demo2();
        System.out.println(demo2.getDemo());
    }

    static class Demo1 {

        private Object queueLock = new Object();

        @GuardedBy(value = "queueLock")
        private List<String> demos;

        public Demo1() {
            demos = new ArrayList<>();
            demos.add("demo1");
        }

        public String getDemo() {
            return Arrays.toString(demos.toArray(new String[demos.size()]));
        }
    }

    static class Demo2 extends Demo1 {

        public Demo2() {
            super();
            try {
                Field field = Demo1.class.getDeclaredField("demos");
                field.setAccessible(true);
                List<String> demos = (List<String>) field.get(this);
                demos.add("demo2");
                field.setAccessible(false);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }

    }

}
