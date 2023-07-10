package bachelor.core.impl.api

import bachelor.core.api.ReactiveJobApiAdapter
import bachelor.core.impl.api.fabric8.Fabric8JobApi
import io.fabric8.kubernetes.client.KubernetesClientBuilder

class Fabric8ReactiveJobApiIT: AbstractReactiveJobApiIT({ namespace ->
    ReactiveJobApiAdapter(Fabric8JobApi(KubernetesClientBuilder().build(), namespace))
})