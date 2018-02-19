package com.alexdeww.rxniosockettcpclientlib

import com.alexdeww.niosockettcpclientlib.PacketProtocol
import io.reactivex.Single

class RxSocketTCPClient<PACKET>(private val host: String,
                                private val port: Int,
                                private val keepAlive: Boolean,
                                private val packetProtocol: PacketProtocol<PACKET>,
                                private val defRequestTimeout: Long = 10) {

    fun createConnectionRequest(): Single<RxTCPConnection<PACKET>> =
            Single.create<RxTCPConnection<PACKET>> { obs ->
                RxTCPConnectionImpl(host, port, keepAlive, packetProtocol, defRequestTimeout)
                        .connect()
                        .subscribe(
                                { if (obs.isDisposed) it.disconnectNow() else obs.onSuccess(it) },
                                { if (!obs.isDisposed) obs.tryOnError(it) }
                        )
            }

}