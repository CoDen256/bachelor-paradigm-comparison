package bachelor.core.impl.api.imperative

import bachelor.core.impl.api.fabric8.Fabric8JobApi
import io.fabric8.kubernetes.client.KubernetesClientBuilder

class Fabric8JobApiIT : AbstractJobApiIT({ namespace ->
    Fabric8JobApi(KubernetesClientBuilder().build(), namespace)
})


