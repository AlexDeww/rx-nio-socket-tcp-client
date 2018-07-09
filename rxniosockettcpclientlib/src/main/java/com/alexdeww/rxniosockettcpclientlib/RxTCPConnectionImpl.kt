package com.alexdeww.rxniosockettcpclientlib

import com.alexdeww.niosockettcpclientlib.*
import com.alexdeww.niosockettcpclientlib.additional.NIOSocketPacketProtocol
import com.alexdeww.niosockettcpclientlib.additional.NIOSocketSerializer
import com.alexdeww.niosockettcpclientlib.core.NIOSocketOperationResult
import com.alexdeww.niosockettcpclientlib.core.NIOSocketWorkerState
import com.alexdeww.rxniosockettcpclientlib.exceptions.*
import io.reactivex.Completable
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.SingleEmitter
import io.reactivex.subjects.CompletableSubject
import io.reactivex.subjects.PublishSubject
import io.reactivex.subjects.SingleSubject
import java.util.concurrent.TimeUnit

internal class RxTCPConnectionImpl<PACKET>(
        host: String,
        port: Int,
        keepAlive: Boolean,
        packetProtocol: NIOSocketPacketProtocol,
        packetSerializer: NIOSocketSerializer<PACKET>,
        private val defRequestTimeout: Long = 10
) : RxTCPConnection<PACKET> {

    private class SendPacketResult<PACKET>(
            val packet: PACKET,
            var emitter: SingleEmitter<PACKET>?
    ) : NIOSocketOperationResult {
        override fun onComplete() {
            emitter?.onSuccess(packet)
        }

        override fun onError(error: Throwable) {
            emitter?.tryOnError(ErrorSendingPacket(error))
        }
    }

    private val tcpSocketClient = NIOSocketTCPClient(host, port, keepAlive, 8192, 5000, packetProtocol, packetSerializer, ClientListener())
    private val _receivedPacketEvent: PublishSubject<PACKET> = PublishSubject.create()
    private var connectionSubj: SingleSubject<RxTCPConnection<PACKET>>? = null
    private var disconnectionSubj: CompletableSubject? = null

    override val receivedPacketEvent: Observable<PACKET> = _receivedPacketEvent

    override fun sendPacket(packet: PACKET): Single<PACKET> = sendPacketEx(packet, defRequestTimeout)

    override fun sendPacketEx(packet: PACKET, requestTimeout: Long): Single<PACKET> = Single.create<PACKET> {
        val sendPacketResult = SendPacketResult(packet, it)
        if (!tcpSocketClient.sendPacket(packet, sendPacketResult)) throw ClientNotConnected()
        it.setCancellable { sendPacketResult.emitter = null }
    }.timeout(requestTimeout, TimeUnit.SECONDS, Single.error { SendPacketTimeout() })

    override fun disconnect(): Completable {
        if (!tcpSocketClient.isConnected) return Completable.complete()
        if (disconnectionSubj != null) return disconnectionSubj!!

        disconnectionSubj = CompletableSubject.create()
        return disconnectionSubj!!.doOnSubscribe { tcpSocketClient.disconnect() }
    }

    override fun disconnectNow() {
        tcpSocketClient.forceDisconnect()
    }

    fun connect(): Single<RxTCPConnection<PACKET>> {
        if (tcpSocketClient.isConnected) return Single.just(this)
        if (connectionSubj != null) return connectionSubj!!

        connectionSubj = SingleSubject.create()
        return connectionSubj!!.doOnSubscribe { tcpSocketClient.connect() }
    }

    private fun doDisconnected() {
        _receivedPacketEvent.onComplete()

        val ds = disconnectionSubj ?: return
        disconnectionSubj = null
        ds.onComplete()
    }

    private fun doConnectionResult(isError: Boolean, error: Throwable? = null) {
        val cs = connectionSubj ?: return
        connectionSubj = null
        if (!isError)
            cs.onSuccess(this)
        else
            cs.onError(error!!)
    }

    private inner class ClientListener : NIOSocketTcpClientListener<PACKET> {
        override fun onConnected(client: NIOSocketTCPClient<PACKET>) {
            doConnectionResult(false)
        }

        override fun onDisconnected(client: NIOSocketTCPClient<PACKET>) {
            doDisconnected()
        }

        override fun onError(client: NIOSocketTCPClient<PACKET>, state: NIOSocketWorkerState, error: Throwable) {
            if (state == NIOSocketWorkerState.CONNECTING) doConnectionResult(true, ConnectionError(error))
        }

        override fun onPacketReceived(client: NIOSocketTCPClient<PACKET>, packet: PACKET) {
            _receivedPacketEvent.onNext(packet)
        }
    }

}