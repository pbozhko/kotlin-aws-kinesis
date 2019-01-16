package com.example

import com.example.util.AwsKinesisManager
import com.example.util.PropertiesManager

fun main(args: Array<String>) {

    val stream = PropertiesManager.getString("aws.kinesis.stream")

    val items = setOf("itemA", "itemB")
    AwsKinesisManager.put(stream, items)
}