import java.io.*
import com.fasterxml.jackson.module.kotlin.*
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec
import com.amazonaws.services.dynamodbv2.document.utils.NameMap
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.ReturnValue
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.fasterxml.jackson.databind.DeserializationFeature
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.fasterxml.jackson.annotation.JsonInclude

const val SQS_STOCK_MANAGER = "stockManagerQ"
const val SQS_PACKING_MANAGER = "packingManagerQ"
const val SQS_SHIPMENT_MANAGER = "shipmentManagerQ"


class AsyncHandler {
    val mapper = jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false) //stops mapper trying (and failing) to deserialise order data
            .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,false)  //allows ignoring of empty properties (specifically for id, as we don't send for notifications to queues)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY) //drops empty fields in serialise (specifically for id, as we don't send for notifications to queues)

    val dynamoDbClient = AmazonDynamoDBClientBuilder
            .standard()
            .withRegion("eu-west-1")
            .build()
    val dynamoDb = DynamoDB(dynamoDbClient)

    fun lambdaHandler(event: SQSEvent){

        for (record in event.records) {
            val rpcObj = mapper.readValue<JsonRpcInput>(record.body)
            when (rpcObj.method) {
                "newOrder" -> {
                    println("Method: $rpcObj.method received, processing ")
                    val sqs = AmazonSQSClientBuilder.defaultClient()
                    val sqsUrl = sqs.getQueueUrl(SQS_STOCK_MANAGER).queueUrl
                    rpcObj.method="stockCheck"
                    println("Sending Method: $rpcObj.method to $SQS_STOCK_MANAGER")
                    val sendMessageQ = SendMessageRequest()
                            .withQueueUrl(sqsUrl)
                            .withMessageBody(mapper.writeValueAsString(rpcObj))
                            .withDelaySeconds(5)
                    sqs.sendMessage(sendMessageQ)
                    //update progress in Db
                    updateOrderDbAttribute(rpcObj.params.ref,"StockCheck","In Progress")

                }

                "stockCheckOK" -> {
                    println("Method: $rpcObj.method received, processing")
                    //mark stock checked in Db
                    updateOrderDbAttribute(rpcObj.params.ref,"StockCheck","OK")
                    //send SQS packing request
                    val sqs = AmazonSQSClientBuilder.defaultClient()
                    val sqsUrl = sqs.getQueueUrl(SQS_PACKING_MANAGER).queueUrl
                    rpcObj.method="packOrder"
                    println("Sending Method: $rpcObj.method to $SQS_PACKING_MANAGER")
                    val sendMessageQ = SendMessageRequest()
                            .withQueueUrl(sqsUrl)
                            .withMessageBody(mapper.writeValueAsString(rpcObj))
                            .withDelaySeconds(5)
                    sqs.sendMessage(sendMessageQ)
                    //update progress in Db
                    updateOrderDbAttribute(rpcObj.params.ref,"Packing","In Progress")
                }

                "packOrderOK" -> {
                    println("Method: $rpcObj.method received, processing")
                    val sqs = AmazonSQSClientBuilder.defaultClient()
                    //mark order packed in Db
                    updateOrderDbAttribute(rpcObj.params.ref,"Packing","OK")
                    //send SQS packing request
                    val sqsUrl = sqs.getQueueUrl(SQS_SHIPMENT_MANAGER).queueUrl
                    rpcObj.method="shipOrder"
                    println("Sending Method: $rpcObj.method to $SQS_SHIPMENT_MANAGER")
                    val sendMessageQ = SendMessageRequest()
                            .withQueueUrl(sqsUrl)
                            .withMessageBody(mapper.writeValueAsString(rpcObj))
                            .withDelaySeconds(5)
                    sqs.sendMessage(sendMessageQ)
                    //update progress in Db
                    updateOrderDbAttribute(rpcObj.params.ref,"Shipping","In Progress")
                }

                "shipOrderOK" -> {
                    println("Method: $rpcObj.method received, processing")
                    //mark order shipped in Db
                    updateOrderDbAttribute(rpcObj.params.ref,"Shipping","OK")
                    //now what ?
                }

                else -> println("Method: $rpcObj.method  is unknown")
            }

        }

        return }

    private fun updateOrderDbAttribute(orderRef: String, attributeName: String, attributeValue: String) {
        val table = dynamoDb.getTable(TABLE_NAME)
        table.updateItem(UpdateItemSpec().withPrimaryKey("orderRef", orderRef)
                .withUpdateExpression("set #attr = :val").withNameMap(NameMap().with("#attr", attributeName))
                .withValueMap(ValueMap().withString(":val", attributeValue)).withReturnValues(ReturnValue.ALL_NEW))

    }


}
