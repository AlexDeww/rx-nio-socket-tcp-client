package com.alexdeww.rxniosockettcpclientlib

import com.alexdeww.niosockettcpclientlib.*
import com.alexdeww.niosockettcpclientlib.additional.NIOSocketPacketHandler
import com.alexdeww.niosockettcpclientlib.additional.NIOSocketPacketProtocol
import com.alexdeww.niosockettcpclientlib.additional.NIOSocketSerializer
import com.alexdeww.rxniosockettcpclientlib.exceptions.*
import io.reactivex.Completable
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.SingleEmitter
import io.reactivex.subjects.CompletableSubject
import io.reactivex.subjects.PublishSubject
import io.reactivex.subjects.SingleSubject
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

internal class RxTCPConnectionImpl<PACKET>(
        host: String,
        port: Int,
        keepAlive: Boolean,
        packetProtocol: NIOSocketPacketProtocol,
        packetSerializer: NIOSocketSerializer<PACKET>,
        private val defRequestTimeout: Long = 10
) : RxTCPConnection<PACKET> {

    private val tcpSocketClient = NIOSocketTCPClient(host, port, keepAlive, SocketHandler(packetProtocol, packetSerializer))
    private val _receivedPacketEvent: PublishSubject<PACKET> = PublishSubject.create()
    private val toSendPacketsPubs = ConcurrentHashMap<PACKET, SingleEmitter<PACKET>>()
    private var connectionSubj: SingleSubject<RxTCPConnection<PACKET>>? = null
    private var disconnectionSubj: CompletableSubject? = null

    override val receivedPacketEvent: Observable<PACKET> = _receivedPacketEvent

    override fun sendPacket(packet: PACKET): Single<PACKET> = sendPacketEx(packet, defRequestTimeout)

    override fun sendPacketEx(packet: PACKET, requestTimeout: Long): Single<PACKET> = Single.create<PACKET> {
        toSendPacketsPubs[packet] = it
        if (!tcpSocketClient.sendData(packet)) {
            toSendPacketsPubs.remove(packet)
            throw ClientNotConnected()
        }
        it.setCancellable { toSendPacketsPubs.remove(packet) }
    }.timeout(requestTimeout, TimeUnit.SECONDS, Single.error {
        toSendPacketsPubs.remove(packet)
        SendPacketTimeout()
    })

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
        toSendPacketsPubs.forEach {
            val pub = it.value
            if (!pub.isDisposed) it.value.tryOnError(Disconnected())
        }
        toSendPacketsPubs.clear()
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

    private inner class SocketHandler(
            protocol: NIOSocketPacketProtocol,
            serializer: NIOSocketSerializer<PACKET>
    ) : NIOSocketPacketHandler<PACKET>(protocol, serializer) {

        override fun onConnected(client: NIOSocketTCPClient<PACKET>) {
            super.onConnected(client)
            doConnectionResult(false)
        }

        override fun onDisconnected(client: NIOSocketTCPClient<PACKET>) {
            super.onDisconnected(client)
            doDisconnected()
        }

        override fun onDataSent(client: NIOSocketTCPClient<PACKET>, data: PACKET) {
            toSendPacketsPubs.remove(data)?.onSuccess(data)
        }

        override fun onDataReceived(client: NIOSocketTCPClient<PACKET>, data: PACKET) {
            _receivedPacketEvent.onNext(data)
        }

        override fun onError(client: NIOSocketTCPClient<PACKET>, clientState: NIOSocketClientState, data: PACKET?, error: Throwable?) {
            when (clientState) {
                NIOSocketClientState.CONNECTING -> doConnectionResult(true, ConnectionError(error))
                NIOSocketClientState.SENDING -> {
                    if (data == null) return
                    toSendPacketsPubs.remove(data)?.also { if (!it.isDisposed) it.tryOnError(ErrorSendingPacket(error)) }
                }
                else -> {  }
            }
        }

    }

}