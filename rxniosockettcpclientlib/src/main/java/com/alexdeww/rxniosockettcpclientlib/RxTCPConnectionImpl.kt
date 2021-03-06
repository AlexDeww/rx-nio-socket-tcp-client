package com.alexdeww.rxniosockettcpclientlib

import com.alexdeww.niosockettcpclientlib.*
import com.alexdeww.niosockettcpclientlib.additional.NIOSocketPacketProtocol
import com.alexdeww.niosockettcpclientlib.additional.NIOSocketSerializer
import com.alexdeww.niosockettcpclientlib.core.NIOSocketOperationResult
import com.alexdeww.niosockettcpclientlib.core.NIOSocketWorkerState
import com.alexdeww.niosockettcpclientlib.core.NIOTcpSocketWorker
import com.alexdeww.rxniosockettcpclientlib.exceptions.*
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.core.SingleEmitter
import io.reactivex.rxjava3.subjects.CompletableSubject
import io.reactivex.rxjava3.subjects.PublishSubject
import io.reactivex.rxjava3.subjects.SingleSubject
import java.util.concurrent.TimeUnit

internal class RxTCPConnectionImpl<PACKET>(
    host: String,
    port: Int,
    keepAlive: Boolean,
    bufferSize: Int = 8192,
    connectionTimeout: Int = 5000,
    packetProtocol: NIOSocketPacketProtocol,
    packetSerializer: NIOSocketSerializer<PACKET>,
    private val defRequestTimeout: Long = 10
) : NIOSocketTCPClient<PACKET>(
    host,
    port,
    keepAlive,
    bufferSize,
    connectionTimeout,
    packetProtocol,
    packetSerializer
), RxTCPConnection<PACKET> {

    private class SendPacketResult<PACKET>(
        val packet: PACKET,
        private var emitter: SingleEmitter<PACKET>?
    ) : NIOSocketOperationResult() {
        override fun onComplete() {
            emitter?.onSuccess(packet)
        }

        override fun onError(error: Throwable) {
            emitter?.tryOnError(ErrorSendingPacket(error))
        }

        override fun cancel() {
            emitter = null
            super.cancel()
        }
    }

    private val _receivedPacketEvent: PublishSubject<PACKET> = PublishSubject.create()
    private var connectionSubj: SingleSubject<RxTCPConnection<PACKET>>? = null
    private var disconnectionSubj: CompletableSubject? = null

    override val receivedPacketEvent: Observable<PACKET> = _receivedPacketEvent

    override fun sendPacket(packet: PACKET): Single<PACKET> =
        sendPacketEx(packet, defRequestTimeout)

    override fun sendPacketEx(packet: PACKET, requestTimeout: Long): Single<PACKET> = Single
        .create<PACKET> {
            val sendPacketResult = SendPacketResult(packet, it)
            if (!super.sendPacket(packet, sendPacketResult)) throw ClientNotConnected()
            it.setCancellable { sendPacketResult.cancel() }
        }
        .timeout(requestTimeout, TimeUnit.SECONDS, Single.error { SendPacketTimeout() })

    override fun close(): Completable {
        if (!isConnected) return Completable.complete()
        if (disconnectionSubj != null) return disconnectionSubj!!

        disconnectionSubj = CompletableSubject.create()
        return disconnectionSubj!!.doOnSubscribe { disconnect() }
    }

    override fun closeNow() {
        forceDisconnect()
    }

    fun open(): Single<RxTCPConnection<PACKET>> {
        if (isConnected) return Single.just(this)
        if (connectionSubj != null) return connectionSubj!!

        connectionSubj = SingleSubject.create()
        return connectionSubj!!.doOnSubscribe { connect() }
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
        if (!isError) cs.onSuccess(this) else cs.onError(error!!)
    }

    override fun onConnected(socket: NIOTcpSocketWorker) {
        super.onConnected(socket)
        doConnectionResult(false)
    }

    override fun onDisconnected(socket: NIOTcpSocketWorker) {
        super.onDisconnected(socket)
        doDisconnected()
    }

    override fun onError(
        socket: NIOTcpSocketWorker,
        state: NIOSocketWorkerState,
        error: Throwable,
        data: ByteArray?
    ) {
        super.onError(socket, state, error, data)
        if (state == NIOSocketWorkerState.CONNECTING) {
            doConnectionResult(true, ConnectionError(error))
        }
    }

    override fun doOnPacketReceived(packet: PACKET) {
        super.doOnPacketReceived(packet)
        _receivedPacketEvent.onNext(packet)
    }

}
