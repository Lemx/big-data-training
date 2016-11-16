import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import spark.ModelAndView;
import spark.Request;
import spark.Response;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static spark.Spark.*;

public class AppMaster {
    private static final Log LOG = LogFactory.getLog(AppMaster.class);
    private static final String MEMORY = "memory";
    private static final String CORES = "cores";
    private static final String CONTAINERS = "containers";
    private static final String PRIORITY = "priority";
    private static final String ITERATIONS = "iterations";
    private static final String PRECISION = "precision";

    private static String appJarPath;
    private static Long appJarTimestamp;
    private static Long appJarPathLen;

    private static final Configuration conf = new YarnConfiguration();
    private static final AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    private static final NMClient nmClient = NMClient.createNMClient();
    private static final CountDownLatch latch = new CountDownLatch(1);
    private static Map<String, String> environment;


    private static LocalResource prepareLocalResource() throws IOException {
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);

        appMasterJar.setType(LocalResourceType.FILE);
        Path jarPath = new Path(appJarPath);
        jarPath = FileSystem.get(conf).makeQualified(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setTimestamp(appJarTimestamp);
        appMasterJar.setSize(appJarPathLen);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);

        return appMasterJar;
    }

    private static ContainerLaunchContext prepareContext(LocalResource appMasterJar, Integer iterations,
                                                         Integer precision, Integer container) {

        ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
        context.setLocalResources(Collections.singletonMap(Constants.JAR_NAME, appMasterJar));
        context.setEnvironment(Collections.singletonMap("CLASSPATH", "./*"));

        List<String> appArgs = new ArrayList<>();

        appArgs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        appArgs.add("PiCalc");
        appArgs.add(iterations.toString());
        appArgs.add(precision.toString());
        appArgs.add(conf.get("fs.defaultFS"));
        appArgs.add(container.toString());
        appArgs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        appArgs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        String command = String.join(" ", appArgs);

        LOG.info("Prepared application command " + command);
        context.setCommands(Collections.singletonList(command));

        return context;
    }

    public static void main(String[] args) throws Exception {
        environment = System.getenv();

        ContainerId containerId = ConverterUtils.toContainerId(environment.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
        ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();

        appJarPath = environment.get(Constants.JAR_PATH);
        appJarTimestamp = Long.valueOf(environment.get(Constants.JAR_TIMESTAMP));
        appJarPathLen = Long.valueOf(environment.get(Constants.JAR_LENGTH));

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId() + ", clusterTimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        runWebServer();
    }


    private static void runWebServer()
            throws InterruptedException {
        port(Integer.parseInt(environment.get(Constants.PORT)));

        HashMap<String, Object> map = new HashMap<>();
        map.put(Constants.MAX_MEM, environment.get(Constants.MAX_MEM));
        map.put(Constants.MAX_CORES, environment.get(Constants.MAX_CORES));

        get("/", (rq, rs) -> new ModelAndView(map, "index"), new ThymeleafTemplateEngine());
        get("/run", AppMaster::run, new ThymeleafTemplateEngine());
        awaitInitialization();

        latch.await();
        Thread.sleep(1000);
        stop();
    }

    private static ModelAndView run(Request rq, Response rs)
            throws IOException, YarnException, InterruptedException {
        Integer memory = Integer.parseInt(rq.queryParams(MEMORY));
        Integer cores = Integer.parseInt(rq.queryParams(CORES));

        final Integer containers = Integer.parseInt(rq.queryParams(CONTAINERS));
        final Integer priority = Integer.parseInt(rq.queryParams(PRIORITY));
        final Integer iterations = Integer.parseInt(rq.queryParams(ITERATIONS));
        final Integer precision = Integer.parseInt(rq.queryParams(PRECISION));

        Integer maxMem = Integer.parseInt(environment.get(Constants.MAX_MEM));
        if (memory > maxMem) {
            memory = maxMem;
        }

        Integer maxCores = Integer.parseInt(environment.get(Constants.MAX_CORES));
        if (cores > maxCores) {
            cores = maxCores;
        }

        LOG.info("Running AppMaster");

        rmClient.init(conf);
        rmClient.start();

        rmClient.registerApplicationMaster("", 0, "");

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(memory);
        capability.setVirtualCores(cores);

        Priority appPriority = Records.newRecord(Priority.class);
        appPriority.setPriority(priority);

        LOG.info("Requesting " + containers + " containers with " + cores + " cores and "
                                                                    + memory + " MB each");
        for (int i = 0; i < containers; i++) {
            ContainerRequest containerReq = new ContainerRequest(capability, null, null, appPriority);
            rmClient.addContainerRequest(containerReq);
        }

        nmClient.init(conf);
        nmClient.start();

        LocalResource appMasterJar = prepareLocalResource();

        Integer allocatedContainers = 0;
        Integer completedContainers = 0;
        while (allocatedContainers < containers) {
            AllocateResponse response = rmClient.allocate(0);
            for (Container container : response.getAllocatedContainers()) {
                allocatedContainers += 1;

                LOG.info("Launching container " + allocatedContainers);
                nmClient.startContainer(container, prepareContext(appMasterJar, iterations,
                                            precision, allocatedContainers));
            }
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                completedContainers += 1;
                LOG.info("ContainerID:" + status.getContainerId() + ", state:" + status.getState().name());
            }
            Thread.sleep(100);
        }

        while (completedContainers < containers) {
            AllocateResponse response = rmClient.allocate(completedContainers / containers);
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                completedContainers += 1;
                LOG.info("ContainerID:" + status.getContainerId() + ", state:" + status.getState().name());
            }
            Thread.sleep(100);
        }

        LOG.info("Completed containers:" + completedContainers);

        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        LOG.info("Finished AppMaster");
        latch.countDown();

        HashMap<String, Object> map = new HashMap<>();
        return new ModelAndView(map, "run");
    }
}