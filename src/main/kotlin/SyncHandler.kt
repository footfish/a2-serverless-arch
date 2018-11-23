import java.io.*
import com.fasterxml.jackson.module.kotlin.*
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.fasterxml.jackson.databind.DeserializationFeature
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.SerializationConfig
import com.fasterxml.jackson.databind.SerializationFeature

data class HandlerInput(val jsonrpc: String, var method: String, var id: String="", val params: OrderData)
data class HandlerResultOutput(val jsonrpc: String, val result: String, val id: String)
data class HandlerErrorOutput(val jsonrpc: String, val error: String, val id: String)
data class OrderData(val order: String, val ref: String)
const val TABLE_NAME = "order"
const val DB_REGION = "eu-west-1"
const val SQS_ASYNC_HANDLER = "asyncHandlerQ"

class SyncHandler {
    val mapper = jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false) //stops mapper trying (and failing) to deserialise order data
            .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,false)  //allows ignoring of empty properties (specifically for id, as we don't send for notifications to queues)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY) //drops empty fields in serialise (specifically for id, as we don't send for notifications to queues)

    val dynamoDbClient = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion(DB_REGION)
            .build()
    val dynamoDb = DynamoDB(dynamoDbClient)

    fun lambdaHandler(input: InputStream, output: OutputStream): Unit {
        val inputObj = mapper.readValue<HandlerInput>(input) //deserialise JSON input stream to inputObj
        val table = dynamoDb.getTable(TABLE_NAME)

        when (inputObj.method) {
            "newOrder" -> {
                //Store order in Db
                try { table.putItem(
                        PutItemSpec()
                                .withItem(Item()
                                        .withPrimaryKey("orderRef", inputObj.params.ref)
                                        .withNumber("timeStamp",System.currentTimeMillis())
                                        .withJSON("order",inputObj.params.order))
                                .withConditionExpression("attribute_not_exists(orderRef)"))
                       mapper.writeValue(output, HandlerResultOutput( "2.0","200 OK",inputObj.id))
                    inputObj.id = "" //id not used in json-rpc notifications
                    //Send newOrder onto Async handler queue
                    val sqs = AmazonSQSClientBuilder.defaultClient()
                    val sqsUrl = sqs.getQueueUrl(SQS_ASYNC_HANDLER).queueUrl
                    val sendMessageQ = SendMessageRequest()
                            .withQueueUrl(sqsUrl)
                            .withMessageBody(mapper.writeValueAsString(inputObj))
                            .withDelaySeconds(5)
                    sqs.sendMessage(sendMessageQ)
                }
                catch (e: Exception){
                    mapper.writeValue(output, HandlerErrorOutput( "2.0","{\"code\": -32601, \"message\": \"${e.message}\"}",inputObj.id))
                   }
            }
            else -> mapper.writeValue(output, HandlerErrorOutput( "2.0","{\"code\": -32601, \"message\": \"Method not found\"}",inputObj.id))
        }

    }
}
