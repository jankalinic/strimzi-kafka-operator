/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;

public class SecretResource implements ResourceType<Secret> {

    @Override
    public String getKind() {
        return TestConstants.SECRET;
    }

    @Override
    public Secret get(String namespace, String name) {
        return ResourceManager.kubeClient().getSecret(namespace, name);
    }

    @Override
    public void create(Secret resource) {
        ResourceManager.kubeClient().createSecret(resource);
    }

    @Override
    public void delete(Secret resource) {
        ResourceManager.kubeClient().deleteSecret(resource.getMetadata().getNamespace(), resource.getMetadata().getName());
    }

    @Override
    public void update(Secret resource) {
        ResourceManager.kubeClient().updateSecret(resource);
    }

    @Override
    public boolean waitForReadiness(Secret resource) {
        return resource != null;
    }
}
