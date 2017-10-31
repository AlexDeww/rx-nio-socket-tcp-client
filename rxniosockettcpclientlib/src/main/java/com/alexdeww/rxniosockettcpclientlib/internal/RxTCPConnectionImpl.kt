package com.alexdeww.rxniosockettcpclientlib.internal

import com.alexdeww.niosockettcpclientlib.NIOSocketTCPClient
import com.alexdeww.niosockettcpclientlib.common.*
import com.alexdeww.rxniosockettcpclientlib.RxTCPConnection
import com.alexdeww.rxniosockettcpclientlib.exceptions.ClientNotConnected
import com.alexdeww.rxniosockettcpclientlib.exceptions.Disconnected
import com.alexdeww.rxniosockettcpclientlib.exceptions.ErrorSendingPacket
import com.alexdeww.rxniosockettcpclientlib.exceptions.SendPacketTimeout
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.SingleEmitter
import io.reactivex.subjects.PublishSubject
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class RxTCPConnectionImpl(host: String,
                          port: Int,
                          keepAlive: Boolean,
                          packetProtocol: PacketProtocol,
                          packetSerializer: PacketSerializer,
                          private val defRequestTimeout: Long = 10,
                          private val connectionListener: ConnectionListener) : RxTCPConnection {

    private val mNetworkClient: NIOSocketTCPClient = NIOSocketTCPClient(host, port, keepAlive,
            packetProtocol, packetSerializer, ConnectionCallbackEvent())
    private val mReceivedPacketEvent: PublishSubject<Packet> = PublishSubject.create()
    private val mToSendPacketsPubs = ConcurrentHashMap<Packet, SingleEmitter<Packet>>()

    init { mNetworkClient.connect() }

    override val receivedPacketEvent: Observable<Packet> = mReceivedPacketEvent

    override fun sendPacket(packet: Packet): Single<Packet> = sendPacketEx(packet, defRequestTimeout)

    override fun sendPacketEx(packet: Packet, requestTimeout: Long): Single<Packet> = Single.create<Packet> {
        if (mNetworkClient.sendPacket(packet)) {
            mToSendPacketsPubs.put(packet, it)
        } else {
            throw ClientNotConnected()
        }
        it.setCancellable { mToSendPacketsPubs.remove(packet) }
    }.timeout(requestTimeout, TimeUnit.SECONDS, Single.error {
        mToSendPacketsPubs.remove(packet)
        throw SendPacketTimeout()
    })

    override fun disconnect() {
        if (mNetworkClient.disconnect()) doDisconnected()
    }

    private fun doDisconnected() {
        mToSendPacketsPubs.forEach {
            val pub = it.value
            if (!pub.isDisposed) it.value.onError(Disconnected())
        }
        mToSendPacketsPubs.clear()
        mReceivedPacketEvent.onComplete()
    }

    private inner class ConnectionCallbackEvent : CallbackEvents {
        override fun onConnected(client: NIOSocketTCPClient) {
            connectionListener.onConnected(this@RxTCPConnectionImpl)
        }

        override fun onDisconnected(client: NIOSocketTCPClient) {
            doDisconnected()
        }

        override fun onPacketSent(client: NIOSocketTCPClient, packet: Packet) {
            val pub = mToSendPacketsPubs.remove(packet) ?: return
            if (!pub.isDisposed) pub.onSuccess(packet)
        }

        override fun onPacketReceived(client: NIOSocketTCPClient, packet: Packet) {
            mReceivedPacketEvent.onNext(packet)
        }

        override fun onError(client: NIOSocketTCPClient, clientState: ClientState, message: String, packet: Packet?) {
            when (clientState) {
                ClientState.CONNECTING -> connectionListener.onConnectionError(message)
                ClientState.SENDING -> {
                    if (packet == null) return
                    val pub = mToSendPacketsPubs.remove(packet) ?: return
                    if (!pub.isDisposed) pub.onError(ErrorSendingPacket(message))
                }
                else -> {  }
            }
        }
    }

    interface ConnectionListener {
        fun onConnected(rxConnection: RxTCPConnection)
        fun onConnectionError(msg: String)
    }

}