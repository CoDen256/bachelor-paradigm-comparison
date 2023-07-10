package bachelor.core.impl.api

import bachelor.core.api.ReactiveJobApiAdapter
import bachelor.core.impl.api.default.DefaultKubernetesClientJobApi
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.util.Config
import java.util.concurrent.TimeUnit

class DefaultKubernetesClientReactiveJobApiIT : AbstractReactiveJobApiIT({ namespace ->
    ReactiveJobApiAdapter(DefaultKubernetesClientJobApi(createApiClient(), namespace))
}) {

    companion object {
        fun createApiClient(): ApiClient {
            return Config.defaultClient().also {
                it.setHttpClient(it.httpClient.newBuilder().readTimeout(0, TimeUnit.SECONDS).build())
            }
        }
    }
}