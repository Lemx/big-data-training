import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Arrays;
import java.util.HashSet;

public class JoinPartitioningPartitioner extends Partitioner<Text,Text> {

    private static final HashSet<OperatingSystem> windows = new HashSet<OperatingSystem>(Arrays.asList(new OperatingSystem[] {
            OperatingSystem.WINDOWS, OperatingSystem.WINDOWS_7, OperatingSystem.WINDOWS_8, OperatingSystem.WINDOWS_10,
            OperatingSystem.WINDOWS_10_MOBILE, OperatingSystem.WINDOWS_81, OperatingSystem.WINDOWS_98,
            OperatingSystem.WINDOWS_2000, OperatingSystem.WINDOWS_MOBILE, OperatingSystem.WINDOWS_MOBILE7,
            OperatingSystem.WINDOWS_PHONE8, OperatingSystem.WINDOWS_PHONE8_1, OperatingSystem.WINDOWS_VISTA,
            OperatingSystem.WINDOWS_XP
    }));

    private static final HashSet<OperatingSystem> android = new HashSet<OperatingSystem>(Arrays.asList(new OperatingSystem[] {
            OperatingSystem.ANDROID, OperatingSystem.ANDROID1, OperatingSystem.ANDROID2, OperatingSystem.ANDROID2_TABLET,
            OperatingSystem.ANDROID3_TABLET, OperatingSystem.ANDROID4, OperatingSystem.ANDROID4_TABLET,
            OperatingSystem.ANDROID4_WEARABLE, OperatingSystem.ANDROID5, OperatingSystem.ANDROID5_TABLET,
            OperatingSystem.ANDROID6, OperatingSystem.ANDROID6_TABLET, OperatingSystem.ANDROID_MOBILE,
            OperatingSystem.ANDROID_MOBILE
    }));

    private static final HashSet<OperatingSystem> apple = new HashSet<OperatingSystem>(Arrays.asList(new OperatingSystem[] {
            OperatingSystem.IOS, OperatingSystem.iOS4_IPHONE, OperatingSystem.iOS5_IPHONE,
            OperatingSystem.iOS6_IPAD, OperatingSystem.iOS6_IPHONE, OperatingSystem.iOS7_IPAD,
            OperatingSystem.iOS7_IPHONE, OperatingSystem.iOS8_1_IPAD, OperatingSystem.iOS8_1_IPHONE,
            OperatingSystem.iOS8_2_IPHONE, OperatingSystem.iOS8_2_IPAD, OperatingSystem.iOS8_3_IPAD,
            OperatingSystem.iOS8_3_IPHONE, OperatingSystem.iOS8_4_IPAD, OperatingSystem.iOS8_4_IPHONE,
            OperatingSystem.iOS8_IPAD, OperatingSystem.iOS8_IPHONE, OperatingSystem.iOS9_IPAD,
            OperatingSystem.iOS9_IPHONE, OperatingSystem.MAC_OS, OperatingSystem.MAC_OS_X,
            OperatingSystem.MAC_OS_X_IPAD, OperatingSystem.MAC_OS_X_IPHONE,
            OperatingSystem.MAC_OS_X_IPOD
    }));

    public int getPartition(Text key, Text value, int numReduceTasks){
        UserAgent userAgent = UserAgent.parseUserAgentString(value.toString());
        if(numReduceTasks==0)
            return 0;
        OperatingSystem os = userAgent.getOperatingSystem();
        if(windows.contains(os)) {
            return 1 % numReduceTasks;
        } else if (android.contains(os)) {
          return 2 % numReduceTasks;
        } else if (apple.contains(os)) {
            return 3 % numReduceTasks;
        } else {
            return 4 % numReduceTasks;
        }
    }
}