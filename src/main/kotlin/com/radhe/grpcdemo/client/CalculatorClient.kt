package com.radhe.grpcdemo.client

import com.proto.calculator.*
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusRuntimeException
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.StreamObserver
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

object CalculatorClient {

    @JvmStatic
    fun main(args: Array<String>) {

        println("Welcome to grpc client")

        // plain text channel
        val channel = ManagedChannelBuilder
            .forAddress("localhost", 7777)
            .usePlaintext()
            .build()

        // secure channel
        val secureChannel = NettyChannelBuilder
            .forAddress("localhost", 7777)
            .sslContext(GrpcSslContexts.forClient().trustManager(File("ssl/ca.crt")).build())
            .build()

        //doUnieryOperation(channel)
        //doServerStreaming(channel)
        //doClientStreaming(channel)
        //doBothSideStreaming(channel)

        //doErrorHandling(channel)
        //doDeadlineHandling(channel)

        // do secure operation
        doUnieryOperation(secureChannel)

        println("Shutting down channel")
        channel.shutdown()
    }

    private fun doDeadlineHandling(channel: ManagedChannel?) {

        val asyncStub = CalculatorServiceGrpc.newBlockingStub(channel)
        val req = HeavyTaskRequest.newBuilder().setTask("Long task").build()
        try {
            val resp = asyncStub.withDeadlineAfter(1, TimeUnit.SECONDS).performHeavyTask(req)
            println("Response received is as -> ${resp.status}")
        } catch (ex: StatusRuntimeException) {
            println("Received exception as -> ")
            ex.printStackTrace()
        }

    }

    private fun doErrorHandling(channel: ManagedChannel?) {
        val number = -4
        val asyncStub = CalculatorServiceGrpc.newBlockingStub(channel)
        val request = SquareRootRequest.newBuilder().setNumber(number).build()

        try {
            val resp = asyncStub.getSquareRoot(request)
            val root = resp.sqRoot
            println("Sq root for the number $number is $root")
        } catch (ex: StatusRuntimeException) {
            println("Exception occurred !!")
            println("status -> ${ex.status}")
            ex.printStackTrace()
        }
    }

    private fun doBothSideStreaming(channel: ManagedChannel) {
        println("creating server stub")
        val asyncStub = CalculatorServiceGrpc.newStub(channel)
        val latch = CountDownLatch(1)
        val requestObserver = asyncStub.getSquareNumber(object : StreamObserver<SquareNumberResponse> {
            override fun onNext(value: SquareNumberResponse) {
                println("Received square is -> ${value.number}")
            }

            override fun onError(t: Throwable?) {
                println("Error received from server")
                latch.countDown()
            }

            override fun onCompleted() {
                println("onCompleted received from server")
                latch.countDown()
            }
        })

        // send number to get square from server
        listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).forEach { num ->
            println("Get square of -> $num")
            val req = SquareNumberRequest.newBuilder().setNumber(num).build()
            requestObserver.onNext(req)
            Thread.sleep(1000)
        }

        // send completed to server
        requestObserver.onCompleted()

        latch.await(3, TimeUnit.SECONDS)
    }

    private fun doClientStreaming(channel: ManagedChannel?) {
        println("creating server stream stub")

        val asyncClient = CalculatorServiceGrpc.newStub(channel)

        val latch = CountDownLatch(1)
        val requestStreamObserver = asyncClient.getMaximumNumber(object : StreamObserver<MaxNumberResponse> {
            override fun onNext(value: MaxNumberResponse) {
                println("Received response from server")
                println("Maximum number is -> ${value.maxNumber}")
                latch.countDown()
            }

            override fun onError(t: Throwable?) {
                latch.countDown()
            }

            override fun onCompleted() {
                latch.countDown()
            }
        })

        listOf<Int>(10, 20, 30, 5, 6, 7, 91, 3, 45).forEach { num ->
            val req = MaxNumberRequest.newBuilder().setNumber(num).build()
            requestStreamObserver.onNext(req)
            Thread.sleep(1000)
        }

        requestStreamObserver.onCompleted()

        latch.await(3, TimeUnit.SECONDS)
    }

    private fun doServerStreaming(channel: ManagedChannel) {

        println("creating server stream stub")
        val asyncClient = CalculatorServiceGrpc.newBlockingStub(channel)
        val primeNumRequest = PrimeNumberRequest.newBuilder()
            .setStartNumber(1)
            .setEndNumber(20)
            .build()

        val itr = asyncClient.getPrimeNumberStream(primeNumRequest)

        itr.forEachRemaining { primeNum ->
            println("Next prime number is -> $primeNum")
        }
    }

    private fun doUnieryOperation(channel: ManagedChannel) {
        println("creating uniery client stub")
        val asyncClient = CalculatorServiceGrpc.newBlockingStub(channel)

        val sumRequest = SumRequest.newBuilder()
            .setFirstNumber(10)
            .setSecondNumber(20)
            .build()

        val sumResponse = asyncClient.getSum(sumRequest)
        println("Sum is as :: ${sumResponse.result}")
    }

}