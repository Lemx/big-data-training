import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hive.com.esotericsoftware.kryo.DefaultSerializer;

import java.util.Arrays;
import java.util.List;


@DefaultSerializer(DummySerializer.class)
@Description(name = "parse_user_agent", value = "Parses user agent into struct <type:STRING, family:STRING, " +
        "os:STRING, device:STRING>")
public class UAParserUDF  extends GenericUDF {

    private PrimitiveObjectInspector objectInspector;

    public ObjectInspector initialize(ObjectInspector[] objectInspectors)
            throws UDFArgumentException {

        objectInspector = (PrimitiveObjectInspector)objectInspectors[0];
        PrimitiveObjectInspector soi = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        List<String> fields =  Arrays.asList("type",
                "family",
                "os",
                "device");

        List<ObjectInspector> ois =  Arrays.asList(new ObjectInspector[] {
                soi,
                soi,
                soi,
                soi
        });

        return ObjectInspectorFactory.getStandardStructObjectInspector(fields, ois);
    }

    public Object evaluate(DeferredObject[] deferredObjects)
            throws HiveException {
        String input = (String) objectInspector.getPrimitiveJavaObject(deferredObjects[0].get());

        if (input == null) {
            return new Object[]{"-", "-", "-", "-"};
        }

        UserAgent userAgent = UserAgent.parseUserAgentString(input);

        String type = userAgent.getBrowser().getBrowserType().getName();
        String family = userAgent.getBrowser().getGroup().getName();
        String os = userAgent.getOperatingSystem().getGroup().getName();
        String device = userAgent.getOperatingSystem().getDeviceType().getName();

        return new Object[] { type, family, os, device };
    }

    public String getDisplayString(String[] strings) {
        return null;
    }
}
