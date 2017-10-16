package com.alexdeww.rxniosockettcpclientlib

import com.alexdeww.niosockettcpclientlib.common.PacketProtocol
import com.alexdeww.niosockettcpclientlib.common.PacketSerializer
import com.alexdeww.rxniosockettcpclientlib.exceptions.ConnectionError
import com.alexdeww.rxniosockettcpclientlib.internal.RxTCPConnectionImpl
import io.reactivex.Observable

class RxSocketTCPClient(private val host: String,
                        private val port: Int,
                        private val keepAlive: Boolean,
                        private val packetProtocol: PacketProtocol,
                        private val packetSerializer: PacketSerializer,
                        private val defRequestTimeout: Long = 10) {

    fun createConnectionRequest(): Observable<RxTCPConnection> =
            Observable.create<RxTCPConnection> { obs ->
                RxTCPConnectionImpl(host, port, keepAlive, packetProtocol, packetSerializer, defRequestTimeout, object : RxTCPConnectionImpl.ConnectionListener {
                    override fun onConnected(rxConnection: RxTCPConnection) {
                        if (obs.isDisposed) {
                            rxConnection.disconnect()
                        } else {
                            obs.onNext(rxConnection)
                        }
                        obs.onComplete()
                    }
                    override fun onConnectionError(msg: String) {
                        if (!obs.isDisposed) obs.onError(ConnectionError(msg))
                    }
                })
            }.share()

}