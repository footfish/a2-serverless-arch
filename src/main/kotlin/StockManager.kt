import com.fasterxml.jackson.module.kotlin.*
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.fasterxml.jackson.databind.DeserializationFeature
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.fasterxml.jackson.annotation.JsonInclude

class StockManager {
    val mapper = jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false) //stops mapper trying (and failing) to deserialise order data
            .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,false)  //allows ignoring of empty properties (specifically for id, as we don't send for notifications to queues)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY) //drops empty fields in serialise (specifically for id, as we don't send for notifications to queues)


    fun lambdaHandler(event: SQSEvent){

        for (record in event.records) {
            val rpcObj = mapper.readValue<JsonRpcInput>(record.body)
            when (rpcObj.method) {
                "newOrder" -> {
                    logIt("Order ${rpcObj.params.ref}, Method: ${rpcObj.method} received, processing")
                }
                "cancelOrder" -> {
                    logIt("Order ${rpcObj.params.ref}, Method: ${rpcObj.method} received, processing")
                }
                "stockCheck" -> {
                    logIt("Order ${rpcObj.params.ref}, Method: ${rpcObj.method} received, processing")
                    val sqs = AmazonSQSClientBuilder.defaultClient()
                    val sqsUrl = sqs.getQueueUrl(SQS_ASYNC_HANDLER).queueUrl
                    if (rpcObj.params.ref.toInt()%5 == 0) {   // make stockCheck fail case
                        rpcObj.method = "stockCheckNOK"
                    } else {
                        rpcObj.method = "stockCheckOK"
                    }
                    logIt("Order ${rpcObj.params.ref}, Sending Method: ${rpcObj.method} to $SQS_ASYNC_HANDLER")
                    val sendMessageQ = SendMessageRequest()
                            .withQueueUrl(sqsUrl)
                            .withMessageBody(mapper.writeValueAsString(rpcObj))
                            .withDelaySeconds(5)
                    sqs.sendMessage(sendMessageQ)
                }
                else -> logIt("Method: ${rpcObj.method} is unknown. Dump:$rpcObj")
            }
        }
        return }

    private fun logIt( message: String){
        println("$LOG_PREFIX:$message")
    }

}
