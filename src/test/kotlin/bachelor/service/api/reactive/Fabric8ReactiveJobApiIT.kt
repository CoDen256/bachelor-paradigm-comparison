package bachelor.service.api.reactive

import bachelor.service.config.fabric8.Fabric8ReactiveJobApi
import io.fabric8.kubernetes.client.KubernetesClientBuilder

class Fabric8ReactiveJobApiIT: AbstractReactiveJobApiIT({ namespace ->
    Fabric8ReactiveJobApi(KubernetesClientBuilder().build(), namespace)
})


