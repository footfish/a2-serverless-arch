import java.io.*
import com.fasterxml.jackson.module.kotlin.*
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import com.amazonaws.services.sns.model.PublishRequest
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.fasterxml.jackson.databind.DeserializationFeature
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.fasterxml.jackson.annotation.JsonInclude
import com.amazonaws.services.sns.model.CreateTopicRequest



data class JsonRpcInput(val jsonrpc: String, var method: String, var id: String="", val params: OrderData)
data class JsonRPCResultOutput(val jsonrpc: String, val result: String, val id: String)
data class JsonRPCErrorOutput(val jsonrpc: String, val error: String, val id: String)
data class OrderData(var order: String="", val ref: String)
const val ORDER_TABLE = "order"
const val SQS_ASYNC_HANDLER = "asyncHandlerQ"
const val SNS_ORDER_TOPIC = "orderTopic"

class SyncHandler {
    val mapper = jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false) //stops mapper trying (and failing) to deserialise order data
            .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,false)  //allows ignoring of empty properties (specifically for id, as we don't send for notifications to queues)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY) //drops empty fields in serialise (specifically for id, as we don't send for notifications to queues)

    fun lambdaHandler(input: InputStream, output: OutputStream) {
        val rpcObj = mapper.readValue<JsonRpcInput>(input) //deserialise JSON input stream to rpcObj
        val orderTable = DynamoDB(AmazonDynamoDBClientBuilder.standard().build()).getTable(ORDER_TABLE)

        when (rpcObj.method) {
            "newOrder" -> {
                //Store order in Db
                try { orderTable.putItem(
                        PutItemSpec()
                                .withItem(Item()
                                        .withPrimaryKey("orderRef", rpcObj.params.ref)
                                        .withNumber("timeStamp",System.currentTimeMillis())
                                        .withJSON("order",rpcObj.params.order))
                                .withConditionExpression("attribute_not_exists(orderRef)"))
                       mapper.writeValue(output, JsonRPCResultOutput( "2.0","200 OK",rpcObj.id))

                    //Send newOrder onto SNS
                    rpcObj.id = "" //clear id (not used in rpc notifications)
                    val sns = AmazonSNSClientBuilder.defaultClient()
                    val createTopicResult = sns.createTopic(CreateTopicRequest(SNS_ORDER_TOPIC)) //idempotent, returns topic ARN if exists
                    sns.publish(PublishRequest(createTopicResult.topicArn, mapper.writeValueAsString(rpcObj)))
                }
                catch (e: Exception){
                    mapper.writeValue(output, JsonRPCErrorOutput( "2.0","{\"code\": -32601, \"message\": \"${e.message}\"}",rpcObj.id))
                   }
            }
            "statusOrder" -> {
                try {
                    //send result, reading order data from Db
                    mapper.writeValue(output, JsonRPCResultOutput("2.0", orderTable.getItem("orderRef", rpcObj.params.ref, null, null).toJSON(), rpcObj.id))
                }
                catch (e: Exception){
                    mapper.writeValue(output, JsonRPCErrorOutput( "2.0","{\"code\": -32601, \"message\": \"${e.message}\"}",rpcObj.id))
                }
            }
            "cancelOrder" -> {
                try {
                    //Check the shipping status in Db
                    val shippingStatus = orderTable.getItem("orderRef", rpcObj.params.ref, "shipping", null).getString("shipping")
                    if (shippingStatus == "Waiting") {
                        //shipping not started proceed to send cancelOrder onto SNS
                        rpcObj.id = "" //clear id (not used in rpc notifications)
                        val sns = AmazonSNSClientBuilder.defaultClient()
                        val createTopicResult = sns.createTopic(CreateTopicRequest(SNS_ORDER_TOPIC)) //idempotent, returns topic ARN if exists
                        sns.publish(PublishRequest(createTopicResult.topicArn, mapper.writeValueAsString(rpcObj)))
                        mapper.writeValue(output, JsonRPCResultOutput( "2.0","200 OK",rpcObj.id))
                    } else {
                        mapper.writeValue(output, JsonRPCErrorOutput("2.0", "{\"code\": -32601, \"message\": \"Can't cancel order\"}", rpcObj.id))
                    }
                }
                catch (e: Exception){
                    mapper.writeValue(output, JsonRPCErrorOutput( "2.0","{\"code\": -32601, \"message\": \"${e.message}\"}",rpcObj.id))
                }
            }
                else -> mapper.writeValue(output, JsonRPCErrorOutput( "2.0","{\"code\": -32601, \"message\": \"Method not found\"}",rpcObj.id))
        }

    }
}
