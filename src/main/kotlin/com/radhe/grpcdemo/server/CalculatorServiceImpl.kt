package com.radhe.grpcdemo.server

import com.google.common.math.IntMath.isPrime
import com.proto.calculator.*
import io.grpc.Context
import io.grpc.Status
import io.grpc.stub.StreamObserver
import java.lang.RuntimeException

class CalculatorServiceImpl : CalculatorServiceGrpc.CalculatorServiceImplBase() {


    override fun getSum(request: SumRequest, responseObserver: StreamObserver<SumResponse>) {
        val firstNum = request.firstNumber
        val secondNum = request.secondNumber

        val sum = firstNum + secondNum

        val response = SumResponse.newBuilder().setResult(sum).build()

        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun getPrimeNumberStream(
        request: PrimeNumberRequest,
        responseObserver: StreamObserver<PrimeNumberResponse>
    ) {
        val startNum = request.startNumber
        val endNum = request.endNumber

        if (startNum < 0 || endNum < 0) {
            val error = RuntimeException("Both Number should be positive")
            responseObserver.onError(error)
        }

        for (num in startNum..endNum) {
            if (isPrime(num)) {
                val resp = PrimeNumberResponse.newBuilder().setPrimeNumber(num).build()
                responseObserver.onNext(resp)
            }
        }

        // send on completion to client
        responseObserver.onCompleted()
    }

    override fun getMaximumNumber(responseObserver: StreamObserver<MaxNumberResponse>): StreamObserver<MaxNumberRequest> {
        return RequestStreamObserver(responseObserver)
    }

    override fun getSquareNumber(responseObserver: StreamObserver<SquareNumberResponse>): StreamObserver<SquareNumberRequest> {
        return object : StreamObserver<SquareNumberRequest> {
            override fun onNext(value: SquareNumberRequest) {
                val sq = value.number * value.number
                val resp = SquareNumberResponse.newBuilder().setNumber(sq).build()
                responseObserver.onNext(resp)
            }

            override fun onError(t: Throwable) {
                responseObserver.onError(RuntimeException("Error received"))
            }

            override fun onCompleted() {
                responseObserver.onCompleted()
            }
        }
    }

    inner class RequestStreamObserver(private val responseObserver: StreamObserver<MaxNumberResponse>) :
        StreamObserver<MaxNumberRequest> {

        private var maxNumberYet = 0

        override fun onNext(value: MaxNumberRequest) {
            val num = value.number
            if (num > maxNumberYet) {
                maxNumberYet = num
            }
        }

        override fun onError(t: Throwable?) {
            responseObserver.onError(RuntimeException("Experienced error during request"))
        }

        override fun onCompleted() {
            val resp = MaxNumberResponse.newBuilder()
                .setMaxNumber(maxNumberYet)
                .build()

            responseObserver.onNext(resp)
            responseObserver.onCompleted()
        }

    }

    override fun getSquareRoot(request: SquareRootRequest, responseObserver: StreamObserver<SquareRootResponse>) {
        val number = request.number

        if (number >= 0) {
            val root = Math.sqrt(number.toDouble())
            responseObserver.onNext(SquareRootResponse.newBuilder().setSqRoot(root).build())
            responseObserver.onCompleted()
        } else {
            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription("Number should not be -ve")
                    .augmentDescription("Number received is $number")
                    .asRuntimeException()
            )
        }
    }

    override fun performHeavyTask(request: HeavyTaskRequest, responseObserver: StreamObserver<HeavyTaskResponse>) {
        val task = request.task

        val current = Context.current()
        try {
            // wait to 3 seconds
            if (current.isCancelled) return else Thread.sleep(3000)
            val resp = HeavyTaskResponse.newBuilder().setStatus("done").build()
            responseObserver.onNext(resp)
            responseObserver.onCompleted()
        } catch (ex: Exception) {
            ex.printStackTrace()
        }
    }
}