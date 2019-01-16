package com.example.util

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput

class KinesisRecordProcessor : IRecordProcessor {

    override fun shutdown(shutdownInput: ShutdownInput?) {
        println(shutdownInput?.shutdownReason)
        println(shutdownInput?.checkpointer)
    }

    override fun initialize(initializationInput: InitializationInput?) {
        println(initializationInput?.shardId)
        println(initializationInput?.extendedSequenceNumber)
        println(initializationInput?.pendingCheckpointSequenceNumber)
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput?) {
        val records = processRecordsInput?.records.orEmpty()
        if(records.isNotEmpty()) {
            records.forEach {
                println(it)
            }
        }
    }
}