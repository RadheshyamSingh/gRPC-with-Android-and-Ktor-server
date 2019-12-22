package com.radhe.grpcdemo.server

import io.grpc.ServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService

object CalculatorServer {

    @JvmStatic
    fun main(args: Array<String>) {

        println("Grpc Server started !!")

        // plaintext server
        val server = ServerBuilder.forPort(7777)
            .addService(CalculatorServiceImpl())
            .addService(ProtoReflectionService.newInstance())
            .build()

        // ssl server
        /*val server = ServerBuilder.forPort(7777)
            .addService(CalculatorServiceImpl())
            .useTransportSecurity(
                File("ssl/server.crt"),
                File("ssl/server.pem")
            )
            .build()*/

        server.start()

        Runtime.getRuntime().addShutdownHook(Thread(Runnable {
            println("Received Shutdown Request")
            server.shutdown()
            println("Server shutdown successfully!!")
        }))
        server.awaitTermination()
    }
}