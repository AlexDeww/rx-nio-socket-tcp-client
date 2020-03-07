package com.alexdeww.rxniosockettcpclientlib

import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single

interface RxTCPConnection<PACKET> {

    /**
     * onNext - emits for each incoming packet from server
     *
     * onComplete - emits if current connection is disconnected
     *
     * onError - never emits
     */
    val receivedPacketEvent: Observable<PACKET>

    /**
     * Send packet to server with default request timeout(10 sec)
     *
     * onNext -> onComplete - emits if packet sent successfully
     *
     * onError - emits if an error occurred(RxConnectionException)
     */
    fun sendPacket(packet: PACKET): Single<PACKET>

    /**
     * Send packet to server with custom request timeout
     *
     * onNext -> onComplete - emits if packet sent successfully
     *
     * onError - emits if an error occurred(RxConnectionException)
     */
    fun sendPacketEx(packet: PACKET, requestTimeout: Long): Single<PACKET>

    /**
     * Disconnect
     */
    fun close(): Completable

    fun closeNow()

}
