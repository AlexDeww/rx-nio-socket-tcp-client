package com.alexdeww.rxniosockettcpclientlib

import com.alexdeww.niosockettcpclientlib.common.Packet
import io.reactivex.Observable
import io.reactivex.Single

interface RxTCPConnection {

    /**
     * onNext - emits for each incoming packet from server
     *
     * onComplete - emits if current connection is disconnected
     *
     * onError - never emits
     */
    val receivedPacketEvent: Observable<Packet>

    /**
     * Send packet to server with default request timeout(10 sec)
     *
     * onNext -> onComplete - emits if packet sent successfully
     *
     * onError - emits if an error occurred(RxConnectionException)
     */
    fun sendPacket(packet: Packet): Single<Packet>

    /**
     * Send packet to server with custom request timeout
     *
     * onNext -> onComplete - emits if packet sent successfully
     *
     * onError - emits if an error occurred(RxConnectionException)
     */
    fun sendPacketEx(packet: Packet, requestTimeout: Long): Single<Packet>

    /**
     * Disconnect
     */
    fun disconnect(force: Boolean = false)
}