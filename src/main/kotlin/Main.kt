import com.google.gson.reflect.TypeToken
import io.fabric8.kubernetes.api.model.Namespace
import io.fabric8.kubernetes.client.*
import io.fabric8.kubernetes.client.informers.ResourceEventHandler
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Namespace
import io.kubernetes.client.util.Config
import io.kubernetes.client.util.Watch as Watch0
import org.apache.logging.log4j.LogManager
import java.lang.Boolean
import java.util.concurrent.TimeUnit
import kotlin.Array
import kotlin.Exception
import kotlin.String

val logger = LogManager.getLogger()
fun main(args: Array<String>) {
    println("Hello World!")

    // Try adding program arguments via Run/Debug configuration.
    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
    println("Program arguments: ${args.joinToString()}")
    informFabric8()
//    watchFabric8()
//    watch()

}

private fun informFabric8(){
    try {
        KubernetesClientBuilder().build().use { client ->
            val items = client.namespaces().list().items
            logger.info("Watch event received")
            val handler = object : ResourceEventHandler<Namespace>{
                override fun onAdd(obj: Namespace?) {
                    println("ADD "+obj)
                }

                override fun onUpdate(oldObj: Namespace?, newObj: Namespace?) {
                    println("UPDATE "+oldObj)

                }

                override fun onDelete(obj: Namespace?, deletedFinalStateUnknown: kotlin.Boolean) {
                    println("DELETE "+obj)

                }

            }
            client.namespaces().inform(handler).use { ignored ->
                Thread.sleep(1000000)
            }

//            client.namespaces().list()
        }
    } catch (e: Exception) {
        logger.error("Global Error: {}", e.message, e)
    }
}

private fun watchFabric8(){
    try {
        KubernetesClientBuilder().build().use { client ->
            val items = client.namespaces().list().items
            logger.info("Watch event received")
            newWatch(client).use { ignored ->
                Thread.sleep(1000000)
            }

//            client.namespaces().list()
        }
    } catch (e: Exception) {
        logger.error("Global Error: {}", e.message, e)
    }
}

private fun newWatch(client: KubernetesClient): Watch {
    return client.namespaces().watch(object : Watcher<Namespace?> {
        override fun eventReceived(action: Watcher.Action, resource: Namespace?) {
            println("Watch event received {}: {}" +  action.name + resource?.metadata?.name)
        }

        override fun onClose(e: WatcherException) {
            println("Watch error received: {}" +  e.message + e)
        }

        override fun onClose() {
            println("Watch gracefully closed")
        }
    })
}


private fun watch() {
    val client = Config.defaultClient()
    // infinite timeout
    // infinite timeout
    val httpClient = client.httpClient.newBuilder().readTimeout(0, TimeUnit.SECONDS).build()
    client.setHttpClient(httpClient)
    Configuration.setDefaultApiClient(client)

    val api = CoreV1Api()

    val watch = Watch0.createWatch<V1Namespace>(
        client,
        api.listNamespaceCall(
            null, null, null, null, null, 5, null,
            null, null,
            Boolean.TRUE, null
        ),
        object : TypeToken<Watch0.Response<V1Namespace?>?>() {}.type
    )

    try {
        for (item in watch) {
            System.out.printf("%s : %s%n", item.type, item.`object`.metadata!!.name)
        }
    } finally {
        watch.close()
    }
}