package com.example.util

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker

object AwsKinesisConsumer {

    private val awsCredentialsProvider: AWSStaticCredentialsProvider

    init {
        val awsCredentials = BasicAWSCredentials(PropertiesManager.getString("aws.access.key"),
            PropertiesManager.getString("aws.secret.key"))

        awsCredentialsProvider = AWSStaticCredentialsProvider(awsCredentials)
    }

    fun run(stream: String) {
        val config = KinesisClientLibConfiguration("streams-adapter-demo",
            stream,
            awsCredentialsProvider,
            "streams-demo-worker")
            .withMaxRecords(1000)
            .withRegionName("us-east-2")
            .withIdleTimeBetweenReadsInMillis(500)
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        val worker = Worker.Builder()
            .recordProcessorFactory(IRecordProcessorFactory {
                KinesisRecordProcessor()
            })
            .config(config)
            .build()
    }
}