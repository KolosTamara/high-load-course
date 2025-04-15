package ru.quipy.payments.logic
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.HdrHistogram.Histogram
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.io.IOException
import java.net.URI
import java.net.SocketTimeoutException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Semaphore
import kotlin.math.max
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private var maxTime = now() - now()
    private var percentile = now()
    private val recentPercentiles = ArrayDeque<Long>(100)
    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofMillis(requestAverageProcessingTime.toMillis()*2))
        .executor(java.util.concurrent.Executors.newFixedThreadPool(1000))
        .build()
    private val histogram = Histogram(1, requestAverageProcessingTime.toMillis() * 2 , 2)
    private val rateLimiter = SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))
    private val semaphore = Semaphore((parallelRequests))
    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
//        if (!rateLimiter.tick()) {
//            logger.warn("Dropped due to rate limit")
//            paymentESService.update(paymentId) {
//                it.logProcessing(false, now(), transactionId, reason = "Dropped due to rate limit")
//            }
//            return
//        }
        val uri = URI.create("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
        val request = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofMillis(requestAverageProcessingTime.toMillis() * 2))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                try {
                    val body = mapper.readValue<ExternalSysResponse>(response.body())
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                } catch (e: Exception) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
            .exceptionally { ex ->
                val reason = when (ex.cause) {
                    is HttpTimeoutException -> "Request timeout."
                    is IOException -> "I/O Error: ${ex.message}"
                    else -> ex.message ?: "Unknown error."
                }
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = reason)
                }
                null
            }
    }
    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}
fun now() = System.currentTimeMillis()
