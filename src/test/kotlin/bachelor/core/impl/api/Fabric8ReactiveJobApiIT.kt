package bachelor.core.impl.api

import bachelor.core.impl.api.fabric8.Fabric8ReactiveJobApi
import io.fabric8.kubernetes.client.KubernetesClientBuilder

class Fabric8ReactiveJobApiIT: AbstractReactiveJobApiIT({ namespace ->
    Fabric8ReactiveJobApi(KubernetesClientBuilder().build(), namespace)
})