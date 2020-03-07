package com.alexdeww.rxniosockettcpclientlib

import com.alexdeww.niosockettcpclientlib.additional.NIOSocketPacketProtocol
import com.alexdeww.niosockettcpclientlib.additional.NIOSocketSerializer
import io.reactivex.rxjava3.core.Single

class RxSocketTCPClient<PACKET>(
    private val host: String,
    private val port: Int,
    private val keepAlive: Boolean,
    private val bufferSize: Int = 8192,
    private val connectionTimeout: Int = 5000,
    private val packetProtocol: NIOSocketPacketProtocol,
    private val packetSerializer: NIOSocketSerializer<PACKET>,
    private val defRequestTimeout: Long = 10
) {

    fun createConnectionRequest(): Single<RxTCPConnection<PACKET>> = Single.create { obs ->
        RxTCPConnectionImpl(
            host,
            port,
            keepAlive,
            bufferSize,
            connectionTimeout,
            packetProtocol,
            packetSerializer,
            defRequestTimeout
        )
            .open()
            .subscribe(
                { if (obs.isDisposed) it.closeNow() else obs.onSuccess(it) },
                { if (!obs.isDisposed) obs.tryOnError(it) }
            )
    }

}
