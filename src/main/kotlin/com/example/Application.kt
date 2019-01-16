package com.example

import com.amazonaws.services.kinesis.model.LimitExceededException
import com.amazonaws.services.kinesis.model.ResourceInUseException
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.example.util.AwsKinesisConsumer
import com.example.util.AwsKinesisManager
import com.example.util.AwsKinesisProducer
import com.example.util.PropertiesManager

fun main(args: Array<String>) {

    val stream = PropertiesManager.getString("aws.kinesis.stream")
    val shardsCount = PropertiesManager.getInt("aws.kinesis.shards")

    //createStream(stream, shardsCount)

    //AwsKinesisConsumer.run(stream)

    putItems(stream)

    //deleteStream(stream)
}

fun putItems(stream: String) {
    val items = setOf("itemA", "itemB")
    AwsKinesisProducer.put(stream, items)
}

fun deleteStream(stream: String) {
    try {
        println(AwsKinesisManager.deleteStream(stream))
    } catch (e: ResourceNotFoundException) {
        println("Cannot delete stream $stream: doesn't exist")
    }
}

fun createStream(stream: String, shardsCount: Int) {
    try {
        println(AwsKinesisManager.createStream(stream, shardsCount))
    } catch (e: ResourceInUseException) {
        println("Cannot create stream $stream: already exists")
    } catch (e: LimitExceededException) {
        println("Cannot create stream $stream: too many shards $shardsCount")
    }
}