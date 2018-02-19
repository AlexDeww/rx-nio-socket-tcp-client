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

internal class RxTCPConnectionImpl<PACKET>(
        host: String,
        port: Int,
        keepAlive: Boolean,
        packetProtocol: PacketProtocol<PACKET>,
        private val defRequestTimeout: Long = 10
) : RxTCPConnection<PACKET> {

    private val mNetworkClient = NIOSocketTCPClient(host, port, keepAlive, packetProtocol, ConnectionCallbackEvent())
    private val mReceivedPacketEvent: PublishSubject<PACKET> = PublishSubject.create()
    private val mToSendPacketsPubs = ConcurrentHashMap<PACKET, SingleEmitter<PACKET>>()
    private var mConnectionSubj: SingleSubject<RxTCPConnection<PACKET>>? = null
    private var mDisconnectionSubj: CompletableSubject? = null

    override val receivedPacketEvent: Observable<PACKET> = mReceivedPacketEvent

    override fun sendPacket(packet: PACKET): Single<PACKET> = sendPacketEx(packet, defRequestTimeout)

    override fun sendPacketEx(packet: PACKET, requestTimeout: Long): Single<PACKET> = Single.create<PACKET> {
        mToSendPacketsPubs[packet] = it
        if (!mNetworkClient.sendPacket(packet)) {
            mToSendPacketsPubs.remove(packet)
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

    fun connect(): Single<RxTCPConnection<PACKET>> {
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

    private inner class ConnectionCallbackEvent : CallbackEvents<PACKET> {
        override fun onConnected(client: NIOSocketTCPClient<PACKET>) {
            doConnectionResult(false)
        }

        override fun onDisconnected(client: NIOSocketTCPClient<PACKET>) {
            doDisconnected()
        }

        override fun onPacketSent(client: NIOSocketTCPClient<PACKET>, packet: PACKET) {
            val pub = mToSendPacketsPubs.remove(packet) ?: return
            pub.onSuccess(packet)
        }

        override fun onPacketReceived(client: NIOSocketTCPClient<PACKET>, packet: PACKET) {
            mReceivedPacketEvent.onNext(packet)
        }

        override fun onError(client: NIOSocketTCPClient<PACKET>, clientState: ClientState, packet: PACKET?, error: Throwable?) {
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