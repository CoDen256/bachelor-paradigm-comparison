package bachelor.service.api.imperative

import bachelor.service.config.fabric8.Fabric8JobApi
import io.fabric8.kubernetes.client.KubernetesClientBuilder

class Fabric8JobApiIT : AbstractJobApiIT({ namespace ->
    Fabric8JobApi(KubernetesClientBuilder().build(), namespace)
})


