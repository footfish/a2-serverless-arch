import com.fasterxml.jackson.module.kotlin.*
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec
import com.amazonaws.services.dynamodbv2.document.utils.NameMap
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.ReturnValue
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import com.amazonaws.services.sns.model.CreateTopicRequest
import com.amazonaws.services.sns.model.PublishRequest
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.fasterxml.jackson.databind.DeserializationFeature
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.fasterxml.jackson.annotation.JsonInclude
import com.amazonaws.services.s3.AmazonS3ClientBuilder


const val SQS_STOCK_MANAGER = "stockManagerQ"
const val SQS_PACKING_MANAGER = "packingManagerQ"
const val SQS_SHIPMENT_MANAGER = "shipmentManagerQ"
const val SNS_EXCEPTION_TOPIC = "orderException"
const val LOG_PREFIX="[LOG]"

class AsyncHandler {
    val mapper = jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false) //stops mapper trying (and failing) to deserialise order data
            .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)  //allows ignoring of empty properties (specifically for id, as we don't send for notifications to queues)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY) //drops empty fields in serialise (specifically for id, as we don't send for notifications to queues)

    fun lambdaHandler(event: SQSEvent) {

        for (record in event.records) {
            val rpcObj = mapper.readValue<JsonRpcInput>(record.body)
            logIt("Method: ${rpcObj.method} received, processing")
            when (rpcObj.method) {
                "newOrder" -> {
                    val sqs = AmazonSQSClientBuilder.defaultClient()
                    val sqsUrl = sqs.getQueueUrl(SQS_STOCK_MANAGER).queueUrl
                    rpcObj.method = "stockCheck"
                    logIt("Order ${rpcObj.params.ref}, Sending Method: ${rpcObj.method} to $SQS_STOCK_MANAGER")
                    val sendMessageQ = SendMessageRequest()
                            .withQueueUrl(sqsUrl)
                            .withMessageBody(mapper.writeValueAsString(rpcObj))
                            .withDelaySeconds(5)
                    sqs.sendMessage(sendMessageQ)
                    //update progress in Db
                    updateOrderDbAttribute(rpcObj.params.ref, "orderStatus", "Active")
                    updateOrderDbAttribute(rpcObj.params.ref, "stockCheck", "In Progress")
                    updateOrderDbAttribute(rpcObj.params.ref, "packing", "Waiting")
                    updateOrderDbAttribute(rpcObj.params.ref, "shipping", "Waiting")
/*                    //log to S3
                    val s3Client = AmazonS3ClientBuilder.defaultClient()
                    // Upload a text string as a new object.
                    val stringObjKeyName="test.txt"
                    val bucketName="orders-xyz-xyz-xyz"
                    s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object"); */

                }
                "stockCheckOK" -> {
                    //mark stock checked in Db
                    updateOrderDbAttribute(rpcObj.params.ref, "stockCheck", "Done")

                    if (orderStatusCheck(rpcObj.params.ref)) { //Don't pack if order is not active
                        //send SQS packing request
                        val sqs = AmazonSQSClientBuilder.defaultClient()
                        val sqsUrl = sqs.getQueueUrl(SQS_PACKING_MANAGER).queueUrl
                        rpcObj.method = "packOrder"
                        logIt("Order ${rpcObj.params.ref}, Sending Method: ${rpcObj.method} to $SQS_PACKING_MANAGER")
                        val sendMessageQ = SendMessageRequest()
                                .withQueueUrl(sqsUrl)
                                .withMessageBody(mapper.writeValueAsString(rpcObj))
                                .withDelaySeconds(5)
                        sqs.sendMessage(sendMessageQ)
                        //update progress in Db
                        updateOrderDbAttribute(rpcObj.params.ref, "packing", "In Progress")
                    } else {
                        logIt("Order ${rpcObj.params.ref}, Sending Method: ${rpcObj.method} to $SQS_PACKING_MANAGER")
                    }
                }
                    "packOrderOK" -> {
                    //mark order packed in Db
                    updateOrderDbAttribute(rpcObj.params.ref, "packing", "Done")

                    if (orderStatusCheck(rpcObj.params.ref)) { //Don't ship if order is not active
                        //send SQS packing request
                        val sqs = AmazonSQSClientBuilder.defaultClient()
                        val sqsUrl = sqs.getQueueUrl(SQS_SHIPMENT_MANAGER).queueUrl
                        rpcObj.method = "shipOrder"
                        logIt("Order ${rpcObj.params.ref}, Sending Method: ${rpcObj.method} to $SQS_SHIPMENT_MANAGER")
                        val sendMessageQ = SendMessageRequest()
                                .withQueueUrl(sqsUrl)
                                .withMessageBody(mapper.writeValueAsString(rpcObj))
                                .withDelaySeconds(5)
                        sqs.sendMessage(sendMessageQ)
                        //update progress in Db
                        updateOrderDbAttribute(rpcObj.params.ref, "shipping", "In Progress")
                    }
                }
                "shipOrderOK" -> {
                    //mark order shipped and completed in Db
                    updateOrderDbAttribute(rpcObj.params.ref, "shipping", "Done")
                    updateOrderDbAttribute(rpcObj.params.ref, "orderStatus", "Completed")
                    logIt("Order ${rpcObj.params.ref}, Completed")
                }
                "stockCheckNOK","packOrderNOK","shipOrderNOK" -> {
                    //Send exception onto SNS
                    val sns = AmazonSNSClientBuilder.defaultClient()
                    val createTopicResult = sns.createTopic(CreateTopicRequest(SNS_EXCEPTION_TOPIC)) //idempotent, returns topic ARN if exists
                    sns.publish(PublishRequest(createTopicResult.topicArn, mapper.writeValueAsString(rpcObj)))
                    //mark stock checked in Db
                    updateOrderDbAttribute(rpcObj.params.ref, "orderStatus", "On Hold")
                    logIt("Order ${rpcObj.params.ref}, Received ${rpcObj.method} order on hold")
                }
                "cancelOrder" -> {
                    updateOrderDbAttribute(rpcObj.params.ref, "shipping", "Cancelled")
                    updateOrderDbAttribute(rpcObj.params.ref, "orderStatus", "Cancelled")
                    logIt("Order ${rpcObj.params.ref}, Cancelled")
                }
                else -> logIt("Method: ${rpcObj.method}  is unknown. Dump:$rpcObj")
            }
        }

        return
    }

    private fun updateOrderDbAttribute(orderRef: String, attributeName: String, attributeValue: String) {
        val orderTable = DynamoDB(AmazonDynamoDBClientBuilder.standard().build()).getTable(ORDER_TABLE)
        orderTable.updateItem(UpdateItemSpec().withPrimaryKey("orderRef", orderRef)
                .withUpdateExpression("set #attr = :val").withNameMap(NameMap().with("#attr", attributeName))
                .withValueMap(ValueMap().withString(":val", attributeValue)).withReturnValues(ReturnValue.ALL_NEW))

    }

    private fun orderStatusCheck(orderRef: String): Boolean {
        val orderTable = DynamoDB(AmazonDynamoDBClientBuilder.standard().build()).getTable(ORDER_TABLE)
        val orderStatus = orderTable.getItem("orderRef", orderRef, "orderStatus", null).getString("orderStatus")
        return (orderStatus == "Active")
    }

    private fun logIt( message: String){
        println("$LOG_PREFIX:$message")
    }
}
