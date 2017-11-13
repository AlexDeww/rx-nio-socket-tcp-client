package com.alexdeww.rxniosockettcpclientlib

import com.alexdeww.niosockettcpclientlib.PacketProtocol
import com.alexdeww.niosockettcpclientlib.PacketSerializer
import io.reactivex.Single

class RxSocketTCPClient(private val host: String,
                        private val port: Int,
                        private val keepAlive: Boolean,
                        private val packetProtocol: PacketProtocol,
                        private val packetSerializer: PacketSerializer,
                        private val defRequestTimeout: Long = 10) {

    fun createConnectionRequest(): Single<RxTCPConnection> =
            Single.create<RxTCPConnection> { obs ->
                RxTCPConnectionImpl(host, port, keepAlive, packetProtocol, packetSerializer, defRequestTimeout)
                        .connect()
                        .subscribe(
                                { if (obs.isDisposed) it.disconnectNow() else obs.onSuccess(it) },
                                { if (!obs.isDisposed) obs.tryOnError(it) }
                        )
            }

}