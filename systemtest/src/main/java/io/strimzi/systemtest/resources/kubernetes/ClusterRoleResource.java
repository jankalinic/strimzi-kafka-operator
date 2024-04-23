/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceType;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ClusterRoleResource implements ResourceType<ClusterRole> {

    @Override
    public String getKind() {
        return TestConstants.CLUSTER_ROLE;
    }
    @Override
    public ClusterRole get(String namespace, String name) {
        return kubeClient().getClusterRole(name);
    }
    @Override
    public void create(ClusterRole resource) {
        // ClusterRole his operation namespace is only 'default'
        kubeClient().createOrUpdateClusterRoles(resource);
    }
    @Override
    public void delete(ClusterRole resource) {
        // ClusterRole his operation namespace is only 'default'
        kubeClient().deleteClusterRole(resource);
    }

    @Override
    public void update(ClusterRole resource) {
        kubeClient().createOrUpdateClusterRoles(resource);
    }

    @Override
    public boolean waitForReadiness(ClusterRole resource) {
        return resource != null;
    }
}
