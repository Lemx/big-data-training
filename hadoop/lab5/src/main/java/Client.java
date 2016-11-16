import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

public class Client {
    private static final Log LOG = LogFactory.getLog(Client.class);

    private static final Configuration conf = new YarnConfiguration();
    private static final YarnClient yarnClient = YarnClient.createYarnClient();
    private static final String applicationName = "lab5";

    private static Integer masterCores;
    private static Integer masterMemory;
    private static String jarPath;

    public static void main(String[] args)
            throws IOException, YarnException {

        if (args.length != 4) {
            System.out.println("Please, specify parameters: <masterCores> <masterMamory> <pathToJar> <port>");
            System.exit(1);
        }

        masterCores = Integer.parseInt(args[0]);
        masterMemory = Integer.parseInt(args[1]);
        jarPath = args[2];
        Integer port = Integer.parseInt(args[3]);

        if (masterMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + masterMemory);
        }
        if (masterCores < 0) {
            throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                    + " Specified virtual cores=" + masterCores);
        }

        LOG.info("Initializing Client");
        yarnClient.init(conf);

        LOG.info("Starting Client");
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        Integer maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        if (masterMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + masterMemory
                    + ", max=" + maxMem);
            masterMemory = maxMem;
        }

        Integer maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

        if (masterCores > maxVCores) {
            LOG.info("AM virtual cores specified above max threshold of cluster. "
                    + "Using max value." + ", specified=" + masterCores
                    + ", max=" + maxVCores);
            masterCores = maxVCores;
        }

        LOG.info("Preparing AppMaster context");
        ApplicationSubmissionContext appContext = prepareContext(app);
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        FileSystem fs = FileSystem.get(conf);

        LOG.info("Preparing local resources");
        LocalResource localResource = prepareLocalResources(appContext, fs);

        Map<String, LocalResource> localResources = new HashMap<>();
        localResources.put(Constants.JAR_NAME, localResource);
        amContainer.setLocalResources(localResources);

        LOG.info("Preparing AppMaster environment");
        Map<String, String> env = prepareEnvironment(fs, localResource);
        env.put(Constants.MAX_MEM, Integer.toString(maxMem - masterMemory)); // sort of hack
        env.put(Constants.MAX_CORES, Integer.toString(maxVCores - masterCores));
        env.put(Constants.PORT, port.toString());
        amContainer.setEnvironment(env);

        LOG.info("Preparing AppMaster command");
        List<String> commands = prepareCommands();
        amContainer.setCommands(commands);

        appContext.setAMContainerSpec(amContainer);

        LOG.info("Submitting application");
        yarnClient.submitApplication(appContext);


    }

    private static ApplicationSubmissionContext prepareContext(YarnClientApplication app) {

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName(applicationName);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(masterMemory);
        capability.setVirtualCores(masterCores);
        appContext.setResource(capability);

        appContext.setQueue("default");
        return appContext;
    }

    private static LocalResource prepareLocalResources(ApplicationSubmissionContext appContext, FileSystem fs)
            throws IOException {

        String suffix = applicationName + "/" + appContext.getApplicationId() + "/" + Constants.JAR_NAME;
        Path dst = new Path(fs.getHomeDirectory(), suffix);

        LOG.info("Copying JAR from " + jarPath + " to " + dst);
        fs.copyFromLocalFile(new Path(jarPath), dst);

        FileStatus scFileStatus = fs.getFileStatus(dst);
        return LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION,
                scFileStatus.getLen(),
                scFileStatus.getModificationTime());
    }

    private static Map<String, String> prepareEnvironment(FileSystem fs, LocalResource scRsrc)
            throws IOException {

        Path hdfsAppJarPath = new Path(fs.getHomeDirectory(), scRsrc.getResource().getFile());
        FileStatus hdfsAppJarStatus = fs.getFileStatus(hdfsAppJarPath);
        long hdfsAppJarLength = hdfsAppJarStatus.getLen();
        long hdfsAppJarTimestamp = hdfsAppJarStatus.getModificationTime();

        Map<String, String> env = new HashMap<>();
        env.put(Constants.JAR_PATH, hdfsAppJarPath.toString());
        env.put(Constants.JAR_TIMESTAMP, Long.toString(hdfsAppJarTimestamp));
        env.put(Constants.JAR_LENGTH, Long.toString(hdfsAppJarLength));


        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                                                .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                                                .append("./*");
        String[] yarnClassPath = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                                    YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH);
        for (String c : yarnClassPath) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        env.put("CLASSPATH", classPathEnv.toString());
        return env;
    }

    private static List<String> prepareCommands() {
        List<String> masterArgs = new ArrayList<>();

        masterArgs.add(Environment.JAVA_HOME.$$() + "/bin/java");
        masterArgs.add("AppMaster");
        masterArgs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        masterArgs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        String command = String.join(" ", masterArgs);

        LOG.info("Prepared AppMaster command " + command);
        return Collections.singletonList(command);
    }
}