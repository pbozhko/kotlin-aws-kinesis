package com.example.util

import java.util.*

object PropertiesManager {

    private val properties = Properties()

    init {
        properties.load(javaClass.classLoader.getResourceAsStream("application.properties"))
    }

    fun getString(key: String): String =
        properties.getValue(key).toString()

    fun getInt(key: String): Int =
        properties.getValue(key).toString().toInt()
}