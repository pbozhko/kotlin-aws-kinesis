package com.example.util

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.CreateStreamRequest
import com.amazonaws.services.kinesis.model.CreateStreamResult
import com.amazonaws.services.kinesis.model.DeleteStreamResult
import com.amazonaws.services.kinesis.model.DescribeStreamRequest

object AwsKinesisManager {

    private val kinesisClient: AmazonKinesis

    init {
        val awsCredentials = BasicAWSCredentials(PropertiesManager.getString("aws.access.key"),
            PropertiesManager.getString("aws.secret.key"))

        kinesisClient = AmazonKinesisClientBuilder.standard()
            .withRegion(Regions.fromName(PropertiesManager.getString("aws.region")))
            .withCredentials(AWSStaticCredentialsProvider(awsCredentials))
            .build()
    }

    fun createStream(stream: String, shardCount: Int): CreateStreamResult {

        val result = kinesisClient.createStream(CreateStreamRequest()
            .withStreamName(stream)
            .withShardCount(shardCount))

        val endTime = System.currentTimeMillis() + 10 * 60 * 1000 // 10 min

        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(1000)
            } catch (e: Exception) {}

            val describeStreamRequest = DescribeStreamRequest().withStreamName(stream)
            val describeStreamResponse = kinesisClient.describeStream(describeStreamRequest)
            val status = describeStreamResponse.streamDescription.streamStatus
            if(status == "ACTIVE") {
                break;
            }
        }

        return result;
    }

    fun deleteStream(stream: String): DeleteStreamResult {
        return kinesisClient.deleteStream(stream)
    }
}