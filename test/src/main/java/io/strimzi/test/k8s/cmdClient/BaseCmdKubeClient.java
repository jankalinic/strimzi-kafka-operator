/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cmdClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.executor.ExecResult;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.join;
import static java.util.Arrays.asList;

public abstract class BaseCmdKubeClient<K extends BaseCmdKubeClient<K>> implements KubeCmdClient<K> {

    private static final Logger LOGGER = LogManager.getLogger(BaseCmdKubeClient.class);

    private static final String GET = "get";
    private static final String CREATE = "create";
    private static final String APPLY = "apply";
    private static final String DELETE = "delete";
    private static final String REPLACE = "replace";
    private static final String PATCH = "patch";

    public static final String CM = "cm";

    String namespace = defaultNamespace();

    public abstract String cmd();

    @Override
    @SuppressWarnings("unchecked")
    public K deleteByName(String namespaceName, String resourceType, String resourceName) {
        Exec.exec(namespacedCommand(namespaceName, DELETE, resourceType, resourceName));
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K deleteAllByResource(String namespaceName, String resourceType) {
        try {
            Exec.exec(namespacedCommand(namespaceName, DELETE, resourceType, "--all"));
        } catch (Exception e) {
            LOGGER.warn(e.getMessage());
        }
        return (K) this;
    }

    protected static class Context implements AutoCloseable {
        @Override
        public void close() { }
    }

    private static final Context NOOP = new Context();

    protected Context defaultContext() {
        return NOOP;
    }

    // Admin contex tis not implemented now, because it's not needed
    // In case it will be neded in future, we should change the kubeconfig and apply it for both oc and kubectl
    protected Context adminContext() {
        return defaultContext();
    }

    protected List<String> defaultNamespaceCommand(String... rest) {
        return namespacedCommand(namespace, asList(rest));
    }

    protected List<String> namespacedCommand(String namespaceName, String... rest) {
        return namespacedCommand(namespaceName, asList(rest));
    }

    private List<String> namespacedCommand(String namespaceName, List<String> rest) {
        List<String> result = new ArrayList<>();
        result.add(cmd());
        result.add("--namespace");
        result.add(namespaceName);
        result.addAll(rest);
        return result;
    }

    protected List<String> namespacelessCommand(String... rest) {
        return namespacelessCommand(asList(rest));
    }

    private List<String> namespacelessCommand(List<String> rest) {
        List<String> result = new ArrayList<>();
        result.add(cmd());
        result.addAll(rest);
        return result;
    }

    @Override
    public String get(String namespaceName, String resource, String resourceName) {
        return Exec.exec(null, namespacedCommand(namespaceName, GET, resource, resourceName, "-o", "yaml"), 0, Level.DEBUG).out();
    }

    @Override
    public String getEvents(String namespaceName) {
        return Exec.exec(null, namespacedCommand(namespaceName, GET, "events"), 0, Level.DEBUG).out();
    }

    @Override
    @SuppressWarnings("unchecked")
    public K create(String namespaceName, File file, boolean localValidation) {
        List<String> command;

        if (Objects.equals(namespaceName, "")) {
            command = namespacedCommand(namespaceName, CREATE, "-f", file.getAbsolutePath());
        } else {
            command = namespacelessCommand(CREATE, "-f", file.getAbsolutePath());
        }


        if (!localValidation) {
            // Disable local CLI validation, delegated to host
            command.add("--validate=false");
        }

        Exec.exec(null, command, 0, Level.DEBUG, true);

        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K createOrReplace(String namespaceName, File file) {
        try (Context context = defaultContext()) {
            try {
                create(namespaceName, file);
            } catch (KubeClusterException.AlreadyExists e) {
                Exec.exec(null, defaultNamespaceCommand(REPLACE, "-f", file.getAbsolutePath()), 0, Level.DEBUG);
            }

            return (K) this;
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public K create(String namespaceName, File... files) {
        try (Context context = defaultContext()) {
            Map<File, ExecResult> execResults = execRecursive(namespaceName, CREATE, files, Comparator.comparing(File::getName).reversed());
            for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
                if (!entry.getValue().exitStatus()) {
                    LOGGER.warn("Failed to create {}!", entry.getKey().getAbsolutePath());
                    LOGGER.debug(entry.getValue().err());
                }
            }
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K apply(String namespaceName, File... files) {
        try (Context context = defaultContext()) {
            Map<File, ExecResult> execResults = execRecursive(namespaceName, APPLY, files, Comparator.comparing(File::getName).reversed());
            for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
                if (!entry.getValue().exitStatus()) {
                    LOGGER.warn("Failed to apply {}!", entry.getKey().getAbsolutePath());
                    LOGGER.debug(entry.getValue().err());
                }
            }
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K delete(String namespaceName, File... files) {
        try (Context context = defaultContext()) {
            Map<File, ExecResult> execResults = execRecursive(namespaceName, DELETE, files, Comparator.comparing(File::getName).reversed());
            for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
                if (!entry.getValue().exitStatus()) {
                    LOGGER.warn("Failed to delete {}!", entry.getKey().getAbsolutePath());
                    LOGGER.debug(entry.getValue().err());
                }
            }
            return (K) this;
        }
    }

    private Map<File, ExecResult> execRecursive(String namespaceName, String subcommand, File[] files, Comparator<File> cmp) {
        Map<File, ExecResult> execResults = new HashMap<>(25);
        for (File f : files) {
            if (f.isFile()) {
                if (f.getName().endsWith(".yaml")) {
                    ExecResult result;

                    if (Objects.equals(namespaceName, "")) {
                        result = Exec.exec(null, namespacedCommand(namespaceName, subcommand, "-f", f.getAbsolutePath()), 0, Level.DEBUG, false);
                    } else {
                        result = Exec.exec(null, namespacelessCommand(subcommand, "-f", f.getAbsolutePath()), 0, Level.DEBUG, false);
                    }

                    execResults.put(f, result);
                }
            } else if (f.isDirectory()) {
                File[] children = f.listFiles();
                if (children != null) {
                    Arrays.sort(children, cmp);
                    execResults.putAll(execRecursive(namespaceName, subcommand, children, cmp));
                }
            } else if (!f.exists()) {
                throw new RuntimeException(new NoSuchFileException(f.getPath()));
            }
        }
        return execResults;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K replace(String namespaceName, File... files) {
        try (Context context = defaultContext()) {
            Map<File, ExecResult> execResults = execRecursive(namespaceName, REPLACE, files, Comparator.comparing(File::getName));
            for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
                if (!entry.getValue().exitStatus()) {
                    LOGGER.warn("Failed to replace {}!", entry.getKey().getAbsolutePath());
                    LOGGER.debug(entry.getValue().err());
                }
            }
            return (K) this;
        }
    }

    @Override
    public K applyContent(String yamlContent) {
        return applyContent(namespace, yamlContent);
    }

    @Override
    @SuppressWarnings("unchecked")
    public K applyContent(String namespaceName, String yamlContent) {
        try (Context context = defaultContext()) {
            Exec.exec(yamlContent, namespacedCommand(namespaceName, APPLY, "-f", "-"), 0, Level.DEBUG);
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K createContent(String yamlContent) {
        try (Context context = defaultContext()) {
            Exec.exec(yamlContent, defaultNamespaceCommand(CREATE, "-f", "-"), 0, Level.DEBUG);
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K replaceContent(String namespaceName, String yamlContent) {
        try (Context context = defaultContext()) {
            try {
                createContent(yamlContent);
            } catch (KubeClusterException.AlreadyExists e) {
                Exec.exec(yamlContent, namespacedCommand(namespaceName, REPLACE, "-f", "-"), 0, Level.DEBUG);
            }

            return (K) this;
        }
    }

    @Override
    public K deleteContent(String yamlContent) {
        return deleteContent(namespace, yamlContent);
    }

    @Override
    @SuppressWarnings("unchecked")
    public K deleteContent(String namespaceName, String yamlContent) {
        try (Context context = defaultContext()) {
            Exec.exec(yamlContent, namespacedCommand(namespaceName, DELETE, "-f", "-"), 0, Level.DEBUG, false);
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K createNamespace(String name) {
        try (Context context = adminContext()) {
            Exec.exec(null, defaultNamespaceCommand(CREATE, "namespace", name), 0, Level.INFO);
        }
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K deleteNamespace(String name) {
        try (Context context = adminContext()) {
            Exec.exec(null, defaultNamespaceCommand(DELETE, "namespace", name), 0, Level.INFO, false);
        }
        return (K) this;
    }

    @Override
    public K scaleByName(String kind, String name, int replicas) {
        return scaleByName(namespace, kind, name, replicas);
    }

    @Override
    @SuppressWarnings("unchecked")
    public K scaleByName(String namespaceName, String kind, String name, int replicas) {
        try (Context context = defaultContext()) {
            Exec.exec(null, namespacedCommand(namespaceName, "scale", kind, name, "--replicas", Integer.toString(replicas)));
            return (K) this;
        }
    }

    @Override
    public ExecResult execInPod(String namespaceName, String pod, String... command) {
        return execInPod(namespaceName, Level.INFO, pod, command);
    }

    @Override
    public ExecResult execInPod(String namespaceName, String pod, boolean throwErrors, String... command) {
        return execInPod(namespaceName, Level.INFO, pod, throwErrors, command);
    }

    @Override
    public ExecResult execInPod(String namespaceName, Level logLevel, String pod, String... command) {
        return execInPod(namespaceName, Level.INFO, pod, true, command);
    }

    @Override
    public ExecResult execInPod(String namespaceName, Level logLevel, String pod, boolean throwErrors, String... command) {
        List<String> cmd = namespacedCommand(namespaceName, "exec", pod, "--");
        cmd.addAll(asList(command));
        return Exec.exec(null, cmd, 0, logLevel, throwErrors);
    }

    @Override
    public ExecResult execInPodContainer(String namespaceName, String pod, String container, String... command) {
        return execInPodContainer(namespaceName, Level.INFO, pod, container, command);
    }

    @Override
    public ExecResult execInPodContainer(String namespaceName, Level logLevel, String pod, String container, String... command) {
        List<String> cmd = namespacedCommand(namespaceName, "exec", pod, "-c", container, "--");
        cmd.addAll(asList(command));
        return Exec.exec(null, cmd, 0, logLevel);
    }

    @Override
    public ExecResult exec(String... command) {
        return exec(true, command);
    }

    @Override
    public ExecResult exec(List<String> command) {
        return exec(true, Level.INFO, command);
    }

    @Override
    public ExecResult exec(boolean throwError, String... command) {
        return exec(throwError, Level.INFO, asList(command));
    }

    @Override
    public ExecResult exec(boolean throwError, Level logLevel, String... command) {
        return exec(throwError, logLevel, asList(command));
    }

    @Override
    public ExecResult exec(boolean throwError, Level logLevel, List<String> command) {
        List<String> cmd = new ArrayList<>();
        cmd.add(cmd());
        cmd.addAll(command);
        return Exec.exec(null, cmd, 0, logLevel, throwError);
    }

    @Override
    public ExecResult execInNamespace(String namespace, String... commands) {
        return Exec.exec(namespacedCommand(namespace, commands));
    }

    @Override
    public ExecResult execInNamespace(String namespaceName, Level logLevel, String... commands) {
        return Exec.exec(null, namespacedCommand(namespaceName, commands), 0, logLevel);
    }

    enum ExType {
        BREAK,
        CONTINUE,
        THROW
    }


    @SuppressWarnings("unchecked")
    public K waitFor(String  namespaceName, String resource, String name, Predicate<JsonNode> condition) {
        long timeoutMs = 570_000L;
        long pollMs = 1_000L;
        ObjectMapper mapper = new ObjectMapper();
        TestUtils.waitFor(resource + " " + name, pollMs, timeoutMs, () -> {
            try {
                String jsonString = Exec.exec(namespacedCommand(namespaceName, "get", resource, name, "-o", "json")).out();
                LOGGER.trace("{}", jsonString);
                JsonNode actualObj = mapper.readTree(jsonString);
                return condition.test(actualObj);
            } catch (KubeClusterException.NotFound e) {
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return (K) this;
    }

    @SuppressWarnings("unchecked")
    public K waitFor(String resource, String name, Predicate<JsonNode> condition) {
        return waitFor(namespace, resource, name, condition);
    }

    @Override
    public K waitForResourceCreation(String namespaceName, String resourceType, String resourceName) {
        // wait when resource to be created
        return waitFor(resourceType, resourceName,
            actualObj -> true
        );
    }

    @Override
    public K waitForResourceCreation(String resourceType, String resourceName) {
        return waitForResourceCreation(namespace, resourceType, resourceName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public K waitForResourceDeletion(String namespaceName, String resourceType, String resourceName) {
        TestUtils.waitFor(resourceType + " " + resourceName + " removal",
            1_000L, 480_000L, () -> {
                try {
                    get(namespaceName, resourceType, resourceName);
                    return false;
                } catch (KubeClusterException.NotFound e) {
                    return true;
                }
            });
        return (K) this;
    }

    public K waitForResourceDeletion(String resourceType, String resourceName) {
        return waitForResourceDeletion(namespace, resourceType, resourceName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public K waitForResourceUpdate(String namespaceName, String resourceType, String resourceName, Date startTime) {

        TestUtils.waitFor(resourceType + " " + resourceName + " update",
                1_000L, 240_000L, () -> {
                try {
                    return startTime.before(getResourceCreateTimestamp(namespaceName, resourceType, resourceName));
                } catch (KubeClusterException.NotFound e) {
                    return false;
                }
            });
        return (K) this;
    }

    @Override
    public Date getResourceCreateTimestamp(String namespaceName, String resourceType, String resourceName) {
        DateFormat df = new SimpleDateFormat("yyyyMMdd'T'kkmmss'Z'");
        Date parsedDate = null;
        try {
            parsedDate = df.parse(JsonPath.parse(getResourceAsJson(namespaceName, resourceType, resourceName)).
                    read("$.metadata.creationTimestamp").toString().replaceAll("\\p{P}", ""));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return parsedDate;
    }

    @Override
    public String toString() {
        return cmd();
    }

    @Override
    public List<String> list(String namespaceName, String resourceType) {
        return asList(Exec.exec(namespacedCommand(GET, resourceType, "-o", "jsonpath={range .items[*]}{.metadata.name} ")).out().trim().split(" +")).stream().filter(s -> !s.trim().isEmpty()).collect(Collectors.toList());
    }

    @Override
    public String getResourceAsJson(String namespaceName, String resourceType, String resourceName) {
        return Exec.exec(namespacedCommand(namespaceName, GET, resourceType, resourceName, "-o", "json")).out();
    }

    @Override
    public String getResourceAsYaml(String namespaceName, String resourceType, String resourceName) {
        return Exec.exec(namespacedCommand(GET, resourceType, resourceName, "-o", "yaml")).out();
    }

    @Override
    public String getResources(String namespaceName, String resourceType) {
        return Exec.exec(namespacedCommand("get", resourceType)).out();
    }

    @Override
    public String getResourcesAsYaml(String namespaceName, String resourceType) {
        return Exec.exec(namespacedCommand(GET, resourceType, "-o", "yaml")).out();
    }

    @Override
    synchronized public void createResourceAndApply(String namespaceName, String template, Map<String, String> params) {
        List<String> cmd = namespacedCommand(namespaceName, "process", template, "-l", "app=" + template, "-o", "yaml");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            cmd.add("-p");
            cmd.add(entry.getKey() + "=" + entry.getValue());
        }

        String yaml = Exec.exec(cmd).out();
        applyContent(yaml);
    }

    @Override
    public String describe(String namespaceName, String resourceType, String resourceName) {
        return Exec.exec(namespacedCommand("describe", resourceType, resourceName)).out();
    }

    @Override
    public String logs(String namespaceName, String pod, String container) {
        String[] args;
        if (container != null) {
            args = new String[]{"logs", pod, "-c", container};
        } else {
            args = new String[]{"logs", pod};
        }
        return Exec.exec(namespacedCommand(namespaceName, args)).out();
    }

    @Override
    public String searchInLog(String namespaceName, String resourceType, String resourceName, long sinceSeconds, String... grepPattern) {
        try {
            return Exec.exec("bash", "-c", join(" ", namespacedCommand("logs", resourceType + "/" + resourceName, "--since=" + sinceSeconds + "s",
                    "|", "grep", " -e " + join(" -e ", grepPattern), "-B", "1"))).out();
        } catch (KubeClusterException e) {
            if (e.result != null && e.result.returnCode() == 1) {
                LOGGER.info("{} not found", grepPattern);
            } else {
                LOGGER.error("Caught exception while searching {} in logs", grepPattern);
            }
        }
        return "";
    }

    @Override
    public String searchInLog(String namespaceName, String resourceType, String resourceName, String resourceContainer, long sinceSeconds, String... grepPattern) {
        try {
            return Exec.exec("bash", "-c", join(" ", namespacedCommand(namespaceName, "logs", resourceType + "/" + resourceName, "-c " + resourceContainer, "--since=" + sinceSeconds + "s",
                    "|", "grep", " -e " + join(" -e ", grepPattern), "-B", "1"))).out();
        } catch (KubeClusterException e) {
            if (e.result != null && e.result.exitStatus()) {
                LOGGER.info("{} not found", grepPattern);
            } else {
                LOGGER.error("Caught exception while searching {} in logs", grepPattern);
            }
        }
        return "";
    }

    public List<String> listResourcesByLabel(String namespaceName, String resourceType, String label) {
        return asList(Exec.exec(namespacedCommand(namespaceName, GET, resourceType, "-l", label, "-o", "jsonpath={range .items[*]}{.metadata.name} ")).out().split("\\s+"));
    }

    @Override
    public String getResourceJsonPath(String namespaceName, String resourceType, String resourceName, String path) {
        return Exec.exec(namespacedCommand(namespaceName, GET, resourceType, "-o", "jsonpath={.items[?(.metadata.name==\"" + resourceName + "\")]" + path + "}")).out().trim();
    }

    @Override
    public boolean getResourceReadiness(String namespaceName, String resourceType, String resourceName) {
        return Exec.exec(namespacedCommand(namespaceName, GET, resourceType, "-o", "jsonpath={.items[?(.metadata.name==\"" + resourceName + "\")].status.conditions[?(.type==\"Ready\")].status}")).out().contains("True");
    }

    @Override
    public void patchResource(String namespaceName, String resourceType, String resourceName, String patchPath, String value) {
        Exec.exec(namespacedCommand(namespaceName, PATCH, resourceType, resourceName, "--type=json", "-p=[{\"op\": \"replace\",\"path\":\"" + patchPath + "\",\"value\":\"" + value + "\"}]"));
    }
}
