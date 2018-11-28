import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.lambda.runtime.events.CloudWatchLogsEvent
import com.amazonaws.util.IOUtils
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.util.zip.GZIPInputStream
import java.io.ByteArrayInputStream
import java.io.IOException
import java.util.*

const val LOG_TABLE = "log"
data class CloudWatchLogsData( val owner: String, val logGroup: String, val logStream: String, val subscriptionFilters: Array<String>, val messageType: String, val logEvents: Array<CloudWatchLogsLogEvent> )
data class CloudWatchLogsLogEvent( val id: String, val message: String, val timestamp: Long)

class Logger {
    val mapper = jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,false)

    val logTable = DynamoDB(AmazonDynamoDBClientBuilder.standard().build()).getTable(LOG_TABLE)

    fun lambdaHandler(event: CloudWatchLogsEvent) {
        val decodedLogEvent = decompress(Base64.getDecoder().decode(event.awsLogs.data))
        val logEventsObj = mapper.readValue<CloudWatchLogsData>(decodedLogEvent) //deserialise JSON log data

        for (logEvent in logEventsObj.logEvents) {
            logTable.putItem(Item().withPrimaryKey("id", logEvent.id)
                    .withString("timestamp", Date(logEvent.timestamp).toString())
                            .withString("message", logEvent.message)
                            .withString("logGroup", logEventsObj.logGroup))
        }
    }
    @Throws(IOException::class)
    private fun decompress(compressed: ByteArray): String {
        val bis = ByteArrayInputStream(compressed)
        val gis = GZIPInputStream(bis)
        val bytes = IOUtils.toByteArray(gis)
        return String(bytes, charset("UTF-8"))
    }

}
