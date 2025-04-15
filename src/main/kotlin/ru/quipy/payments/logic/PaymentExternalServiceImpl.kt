package ru.quipy.payments.logic
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.HdrHistogram.Histogram
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.max
// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
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
    private var callTimeout = properties.averageProcessingTime.toMillis() * 2
    private var client = OkHttpClient.Builder().protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .connectionPool(ConnectionPool(1000, 1, TimeUnit.MINUTES))
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .callTimeout(Duration.ofMillis(requestAverageProcessingTime.toMillis() * 2))
        .build()
    private val histogram = Histogram(1, requestAverageProcessingTime.toMillis() * 2 , 2)
    private val rateLimiter = SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))
    private val semaphore = Semaphore((parallelRequests))
    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        //logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        //logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")
        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()
//        val processingTimeMillis = deadline - now()
//        var shouldRetry = true;
        if (!rateLimiter.tick()) {
            logger.warn("Dropped due to rate limit")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Dropped due to rate limit")
            }
            return
        }
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                val reason = when (e) {
                    is SocketTimeoutException -> "Request timeout."
                    else -> e.message ?: "Unknown error."
                }
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = reason)
                }
            }
            override fun onResponse(call: Call, response: Response) {
                response.use {
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            }
        })
    }
    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}
public fun now() = System.currentTimeMillis()
