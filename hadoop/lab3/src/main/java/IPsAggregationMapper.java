import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class IPsAggregationMapper extends Mapper<Object, Text, Text, IntLongWritablePair> {

    private Text outputKey = new Text();
    private IntLongWritablePair outputValue = new IntLongWritablePair();

    private static final String regex = "^(\\w+)\\s[\\w.-]+\\s[\\w.-]+\\s\\[.*\\]\\s\".*\"\\s\\d{3}\\s(\\d+|-)\\s\".*\"\\s\"(.*)\"$";
    private static final Pattern pattern = Pattern.compile(regex);

    private static final HashSet<Browser> ieKinds = new HashSet<Browser>(Arrays.asList(new Browser[] {
            Browser.IE, Browser.IE5, Browser.IE5_5, Browser.IE6, Browser.IE7, Browser.IE8, Browser.IE9,
            Browser.IE10, Browser.IE11, Browser.IE_XBOX, Browser.IEMOBILE6, Browser.IEMOBILE7, Browser.IEMOBILE9,
            Browser.IEMOBILE10, Browser.IEMOBILE11
    }));

    private static final HashSet<Browser> mozillaKinds = new HashSet<Browser>(Arrays.asList(new Browser[] {
            Browser.MOZILLA, Browser.FIREFOX, Browser.FIREFOX1_5, Browser.FIREFOX2, Browser.FIREFOX3,
            Browser.FIREFOX3MOBILE, Browser.FIREFOX4, Browser.FIREFOX5,Browser.FIREFOX6, Browser.FIREFOX7,
            Browser.FIREFOX8, Browser.FIREFOX9, Browser.FIREFOX10, Browser.FIREFOX11, Browser.FIREFOX12,
            Browser.FIREFOX13, Browser.FIREFOX14, Browser.FIREFOX15, Browser.FIREFOX16, Browser.FIREFOX17,
            Browser.FIREFOX18, Browser.FIREFOX19, Browser.FIREFOX20, Browser.FIREFOX21, Browser.FIREFOX22,
            Browser.FIREFOX23, Browser.FIREFOX24, Browser.FIREFOX25, Browser.FIREFOX26, Browser.FIREFOX27,
            Browser.FIREFOX28, Browser.FIREFOX29, Browser.FIREFOX30, Browser.FIREFOX31, Browser.FIREFOX32,
            Browser.FIREFOX33, Browser.FIREFOX34, Browser.FIREFOX35, Browser.FIREFOX36, Browser.FIREFOX37,
            Browser.FIREFOX38, Browser.FIREFOX39, Browser.FIREFOX40, Browser.FIREFOX41, Browser.FIREFOX42,
            Browser.FIREFOX43, Browser.FIREFOX44, Browser.FIREFOX45, Browser.FIREFOX46, Browser.FIREFOX47,
            Browser.FIREFOX48, Browser.FIREFOX_MOBILE, Browser.FIREFOX_MOBILE23, Browser.FIREFOX_MOBILE_IOS
    }));

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String ipString;
        Long bytesCount;

        Matcher matcher = pattern.matcher(value.toString());
        if (matcher.matches()) {
            ipString = matcher.group(1);
            String bytesString = matcher.group(2);
            if (!bytesString.equals("-")) { // skipping entries with no bytes
                Browsers.KIND browser = getBrowser(matcher.group(3).toLowerCase());
                context.getCounter(browser).increment(1);
                bytesCount = Long.parseLong(bytesString);
                outputKey.set(ipString);
                outputValue.set(1, bytesCount);
                context.write(outputKey, outputValue);
            }
        }
    }

    private Browsers.KIND getBrowser(String agentInfo) {
        UserAgent userAgent = UserAgent.parseUserAgentString(agentInfo);
        if (ieKinds.contains(userAgent.getBrowser())) {
            return Browsers.KIND.IE;
        }
        else if (mozillaKinds.contains(userAgent.getBrowser())){
            return Browsers.KIND.MOZILLA;
        }
        else {
            return Browsers.KIND.OTHER;
        }
    }
}
