/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cmdClient;

import com.fasterxml.jackson.databind.JsonNode;
import io.strimzi.test.executor.ExecResult;
import org.apache.logging.log4j.Level;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Abstraction for a kubernetes client.
 * @param <K> The subtype of KubeClient, for fluency.
 */
public interface KubeCmdClient<K extends KubeCmdClient<K>> {

    String defaultNamespace();

    KubeCmdClient<K> namespace(String namespace);

    /** Returns namespace for cluster */
    String namespace();

    /** Creates the resources in the given files. */
    K create(String namespace, File... files);

    default K create(String namespaceName, File file) {
        return create(namespaceName, file, true);
    }

    K create(String namespaceName, File file, boolean localValidation);
    default K create(File file, boolean localValidation) {
        return create("", file, localValidation);
    }

    default K create(String namespaceName, String filePath) {
        return create(namespaceName, asList(filePath).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    default K createFromFile(String filePath) {
        return create("", filePath);
    }

    /** Creates the resources in the given files. */
    K apply(String namespaceName, File... files);

    default K apply(String namespaceName, String... files) {
        return apply(namespaceName, asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    /** Deletes the resources in the given files. */
    K delete(String namespaceName, File... files);

    default K delete(File... files) {
        return delete("", files);
    }

    default K delete(String namespaceName, String... files) {
        return delete(namespaceName, asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    default K deleteFromFile(String... files) {
        return delete("", files);
    }

    /** Deletes the resources by resource name. */
    K deleteByName(String namespaceName, String resourceType, String resourceName);

    K deleteAllByResource(String namespaceName, String resourceType);

    /** Replaces the resources in the given files. */
    K replace(String namespaceName, File... files);

    default K replace(String namespaceName, String... files) {
        return replace(namespaceName, asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    K createOrReplace(String namespaceName, File file);

    K applyContent(String yamlContent);

    K applyContent(String namespaceName, String yamlContent);

    K createContent(String yamlContent);

    K replaceContent(String namespaceName, String yamlContent);

    K deleteContent(String yamlContent);

    K deleteContent(String namespaceName, String yamlContent);

    K createNamespace(String name);

    K deleteNamespace(String name);

    /**
     * Scale resource using the scale subresource
     *
     * @param kind      Kind of the resource which should be scaled
     * @param name      Name of the resource which should be scaled
     * @param replicas  Number of replicas to which the resource should be scaled
     * @return          This kube client
     */
    K scaleByName(String kind, String name, int replicas);
    K scaleByName(String namespaceName, String kind, String name, int replicas);

    /**
     * Execute the given {@code command} in the given {@code pod}.
     * @param pod The pod
     * @param command The command
     * @return The process result.
     */
    ExecResult execInPod(String namespaceName, String pod, String... command);

    ExecResult execInPod(String namespaceName, String pod, boolean throwErrors, String... command);

    ExecResult execInPod(String namespaceName, Level logLevel, String pod, String... command);

    ExecResult execInPod(String namespaceName, Level logLevel, String pod, boolean throwErrors, String... command);

    ExecResult execInNamespace(String namespaceName, String... commands);

    ExecResult execInNamespace(String namespaceName, Level logLevel, String... commands);

    /**
     * Execute the given {@code command} in the given {@code container} which is deployed in {@code pod}.
     * @param pod The pod
     * @param container The container
     * @param command The command
     * @return The process result.
     */
    ExecResult execInPodContainer(String namespaceName, String pod, String container, String... command);

    ExecResult execInPodContainer(String namespaceName, Level logLevel, String pod, String container, String... command);

    /**
     * Execute the given {@code command}.
     * @param command The command
     * @return The process result.
     */
    ExecResult exec(String... command);

    /**
     * Execute the given {@code command}.
     * @param command The command
     * @return The process result.
     */
    ExecResult exec(List<String> command);

    /**
     * Execute the given {@code command}. You can specify if potential failure will thrown the exception or not.
     * @param throwError parameter which control thrown exception in case of failure
     * @param command The command
     * @return The process result.
     */
    ExecResult exec(boolean throwError, String... command);

    /**
     * Execute the given {@code command}. You can specify if potential failure will thrown the exception or not.
     * @param throwError parameter which control thrown exception in case of failure
     * @param command The command
     * @param logLevel determines log level where the output will be printed to
     * @return The process result.
     */
    ExecResult exec(boolean throwError, Level logLevel, String... command);

    /**
     * Execute the given {@code command}. You can specify if potential failure will thrown the exception or not.
     * @param throwError parameter which control thrown exception in case of failure
     * @param command The command
     * @param logLevel determines log level where the output will be printed to
     * @return The process result.
     */
    ExecResult exec(boolean throwError, Level logLevel, List<String> command);

    /**
     * Wait for the resource with the given {@code name} to be reach the state defined by the predicate.
     * @param resource The resource type.
     * @param name The resource name.
     * @param condition Predicate to test if the desired state was achieved
     * @return This kube client.
     */
    K waitFor(String resource, String name, Predicate<JsonNode> condition);
    K waitFor(String namespaceName, String resource, String name, Predicate<JsonNode> condition);

    /**
     * Wait for the resource with the given {@code name} to be created.
     * @param resourceType The resource type.
     * @param resourceName The resource name.
     * @return This kube client.
     */
    K waitForResourceCreation(String resourceType, String resourceName);
    K waitForResourceCreation(String namespaceName, String resourceType, String resourceName);

    /**
     * Get the content of the given {@code resource} with the given {@code name} as YAML.
     * @param namespaceName The name of namespace.
     * @param resource The type of resource (e.g. "cm").
     * @param resourceName The name of the resource.
     * @return The resource YAML.
     */
    String get(String namespaceName, String resource, String resourceName);

    /**
     * Get a list of events in a given namespace
     * @return List of events
     */
    String getEvents(String namespaceName);

    K waitForResourceDeletion(String namespaceName, String resourceType, String resourceName);
    K waitForResourceDeletion(String resourceType, String resourceName);

    List<String> list(String namespaceName, String resourceType);

    String getResourceAsYaml(String namespaceName, String resourceType, String resourceName);

    String getResources(String namespaceName, String resourceType);

    String getResourcesAsYaml(String namespaceName, String resourceType);

    void createResourceAndApply(String namespaceName, String template, Map<String, String> params);

    String describe(String namespaceName, String resourceType, String resourceName);

    default String logs(String namespaceName, String pod) {
        return logs(namespaceName, pod, null);
    }

    String logs(String namespaceName, String pod, String container);

    /**
     * @param resourceType The type of resource
     * @param resourceName The name of resource
     * @param sinceSeconds Return logs newer than a relative duration like 5s, 2m, or 3h.
     * @param grepPattern Grep patterns for search
     * @return Grep result as string
     */
    String searchInLog(String namespaceName, String resourceType, String resourceName, long sinceSeconds, String... grepPattern);

    /**
     * @param resourceType The type of resource
     * @param resourceName The name of resource
     * @param resourceContainer The name of resource container
     * @param sinceSeconds Return logs newer than a relative duration like 5s, 2m, or 3h.
     * @param grepPattern Grep patterns for search
     * @return Grep result as string
     */
    String searchInLog(String namespaceName, String resourceType, String resourceName, String resourceContainer, long sinceSeconds, String... grepPattern);

    String getResourceAsJson(String namespaceName, String resourceType, String resourceName);

    K waitForResourceUpdate(String namespaceName, String resourceType, String resourceName, Date startTime);

    Date getResourceCreateTimestamp(String namespaceName, String pod, String s);

    List<String> listResourcesByLabel(String namespaceName, String resourceType, String label);

    String cmd();

    String getResourceJsonPath(String namespaceName, String resourceType, String resourceName, String path);

    boolean getResourceReadiness(String namespaceName, String resourceType, String resourceName);

    void patchResource(String namespaceName, String resourceType, String resourceName, String patchPath, String value);
}
