package com.example.util

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.PutRecordsRequest
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry
import com.amazonaws.services.kinesis.model.PutRecordsResult
import java.nio.ByteBuffer

object AwsKinesisProducer {

    private val kinesisClient: AmazonKinesis

    init {
        val awsCredentials = BasicAWSCredentials(PropertiesManager.getString("aws.access.key"),
            PropertiesManager.getString("aws.secret.key"))

        kinesisClient = AmazonKinesisClientBuilder.standard()
            .withRegion(Regions.fromName(PropertiesManager.getString("aws.region")))
            .withCredentials(AWSStaticCredentialsProvider(awsCredentials))
            .build()
    }

    fun put(stream: String, items: Set<String>): PutRecordsResult {
        val putRecordsRequest = PutRecordsRequest()
        putRecordsRequest.setStreamName(stream)

        val records = ArrayList<PutRecordsRequestEntry>()

        items.forEachIndexed { index, it ->
            val entry = PutRecordsRequestEntry()
            entry.data = ByteBuffer.wrap(it.toByteArray())
            entry.partitionKey = "key-$index"
            records.add(entry)
        }

        putRecordsRequest.setRecords(records);

        return kinesisClient.putRecords(putRecordsRequest);
    }
}