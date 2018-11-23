import java.io.*
import com.fasterxml.jackson.module.kotlin.*
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec
import com.amazonaws.services.dynamodbv2.document.utils.NameMap
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.ReturnValue
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.SNSEvent
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.fasterxml.jackson.databind.DeserializationFeature
import java.util.HashMap
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage
import com.fasterxml.jackson.annotation.JsonInclude

//data class HandlerInput(val jsonrpc: String, val method: String, val id: String, val params: OrderData)
//data class HandlerResultOutput(val jsonrpc: String, val result: String, val id: String)
//data class HandlerErrorOutput(val jsonrpc: String, val error: String, val id: String)
//data class OrderData(val order: String, val ref: String)
//const val TABLE_NAME = "order"
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
            val inputObj = mapper.readValue<HandlerInput>(record.body)
            when (inputObj.method) {
                "newOrder" -> {
                    println("Method: $inputObj.method received, processing ")
                    val sqs = AmazonSQSClientBuilder.defaultClient()
                    val sqsUrl = sqs.getQueueUrl(SQS_STOCK_MANAGER).queueUrl
                    inputObj.method="stockCheck"
                    println("Sending Method: $inputObj.method to $SQS_STOCK_MANAGER")
                    val sendMessageQ = SendMessageRequest()
                            .withQueueUrl(sqsUrl)
                            .withMessageBody(mapper.writeValueAsString(inputObj))
                            .withDelaySeconds(5)
                    sqs.sendMessage(sendMessageQ)
                    //update progress in Db
                    updateOrderDbAttribute(inputObj.params.ref,"StockCheck","In Progress")

                }

                "stockCheckOK" -> {
                    println("Method: $inputObj.method received, processing")
                    //mark stock checked in Db
                    updateOrderDbAttribute(inputObj.params.ref,"StockCheck","OK")
                    //send SQS packing request
                    val sqs = AmazonSQSClientBuilder.defaultClient()
                    val sqsUrl = sqs.getQueueUrl(SQS_PACKING_MANAGER).queueUrl
                    inputObj.method="packOrder"
                    println("Sending Method: $inputObj.method to $SQS_PACKING_MANAGER")
                    val sendMessageQ = SendMessageRequest()
                            .withQueueUrl(sqsUrl)
                            .withMessageBody(mapper.writeValueAsString(inputObj))
                            .withDelaySeconds(5)
                    sqs.sendMessage(sendMessageQ)
                    //update progress in Db
                    updateOrderDbAttribute(inputObj.params.ref,"Packing","In Progress")
                }

                "packOrderOK" -> {
                    println("Method: $inputObj.method received, processing")
                    val sqs = AmazonSQSClientBuilder.defaultClient()
                    //mark order packed in Db
                    updateOrderDbAttribute(inputObj.params.ref,"Packing","OK")
                    //send SQS packing request
                    val sqsUrl = sqs.getQueueUrl(SQS_SHIPMENT_MANAGER).queueUrl
                    inputObj.method="shipOrder"
                    println("Sending Method: $inputObj.method to $SQS_SHIPMENT_MANAGER")
                    val sendMessageQ = SendMessageRequest()
                            .withQueueUrl(sqsUrl)
                            .withMessageBody(mapper.writeValueAsString(inputObj))
                            .withDelaySeconds(5)
                    sqs.sendMessage(sendMessageQ)
                    //update progress in Db
                    updateOrderDbAttribute(inputObj.params.ref,"Shipping","In Progress")
                }

                "shipOrderOK" -> {
                    println("Method: $inputObj.method received, processing")
                    //mark order shipped in Db
                    updateOrderDbAttribute(inputObj.params.ref,"Shipping","OK")
                    //now what ?
                }

                else -> println("Method: $inputObj.method  is unknown")
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
