import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.Serializer;
import org.apache.hive.com.esotericsoftware.kryo.io.Input;
import org.apache.hive.com.esotericsoftware.kryo.io.Output;

/**
 * Workaround for known issue
 */
public class DummySerializer extends Serializer {

    public void write(Kryo kryo, Output output, Object o) {

    }

    public Object read(Kryo kryo, Input input, Class aClass) {
        try {
            return aClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}
