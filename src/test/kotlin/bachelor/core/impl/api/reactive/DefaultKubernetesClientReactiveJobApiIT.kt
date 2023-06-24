package bachelor.core.impl.api.reactive

import bachelor.core.impl.api.AbstractReactiveJobApiIT
import bachelor.core.impl.api.default.DefaultKubernetesClientReactiveJobApi
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.util.Config
import java.util.concurrent.TimeUnit

class DefaultKubernetesClientReactiveJobApiIT : AbstractReactiveJobApiIT({ namespace ->
    DefaultKubernetesClientReactiveJobApi(createApiClient(), namespace)
}) {

    companion object {
        fun createApiClient(): ApiClient {
            return Config.defaultClient().also {
                it.setHttpClient(it.httpClient.newBuilder().readTimeout(0, TimeUnit.SECONDS).build())
            }
        }
    }
}


