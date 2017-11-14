package com.alexdeww.rxniosockettcpclientlib

import com.alexdeww.niosockettcpclientlib.*
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

internal class RxTCPConnectionImpl(host: String,
                                   port: Int,
                                   keepAlive: Boolean,
                                   packetProtocol: PacketProtocol,
                                   packetSerializer: PacketSerializer,
                                   private val defRequestTimeout: Long = 10) : RxTCPConnection {

    private val mNetworkClient: NIOSocketTCPClient = NIOSocketTCPClient(host, port, keepAlive,
            packetProtocol, packetSerializer, ConnectionCallbackEvent())
    private val mReceivedPacketEvent: PublishSubject<Packet> = PublishSubject.create()
    private val mToSendPacketsPubs = ConcurrentHashMap<Packet, SingleEmitter<Packet>>()
    private var mConnectionSubj: SingleSubject<RxTCPConnection>? = null
    private var mDisconnectionSubj: CompletableSubject? = null

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
        SendPacketTimeout()
    })

    override fun disconnect(): Completable {
        if (!mNetworkClient.isConnected) return Completable.complete()
        if (mDisconnectionSubj != null) return mDisconnectionSubj!!

        mDisconnectionSubj = CompletableSubject.create()
        return mDisconnectionSubj!!.doOnSubscribe { mNetworkClient.disconnect() }
    }

    override fun disconnectNow() {
        mNetworkClient.forceDisconnect()
    }

    fun connect(): Single<RxTCPConnection> {
        if (mNetworkClient.isConnected) return Single.just(this)
        if (mConnectionSubj != null) return mConnectionSubj!!

        mConnectionSubj = SingleSubject.create()
        return mConnectionSubj!!.doOnSubscribe { mNetworkClient.connect() }
    }

    private fun doDisconnected() {
        mToSendPacketsPubs.forEach {
            val pub = it.value
            if (!pub.isDisposed) it.value.tryOnError(Disconnected())
        }
        mToSendPacketsPubs.clear()
        mReceivedPacketEvent.onComplete()

        val ds = mDisconnectionSubj ?: return
        mDisconnectionSubj = null
        ds.onComplete()
    }

    private fun doConnectionResult(isError: Boolean, error: Throwable? = null) {
        val cs = mConnectionSubj ?: return
        mConnectionSubj = null
        if (!isError)
            cs.onSuccess(this)
        else
            cs.onError(error!!)
    }

    private inner class ConnectionCallbackEvent : CallbackEvents {
        override fun onConnected(client: NIOSocketTCPClient) {
            doConnectionResult(false)
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

        override fun onError(client: NIOSocketTCPClient, clientState: ClientState, packet: Packet?, error: Throwable?) {
            when (clientState) {
                ClientState.CONNECTING -> doConnectionResult(true, ConnectionError(error))
                ClientState.SENDING -> {
                    if (packet == null) return
                    val pub = mToSendPacketsPubs.remove(packet) ?: return
                    if (!pub.isDisposed) pub.tryOnError(ErrorSendingPacket(error))
                }
                else -> {  }
            }
        }
    }

}