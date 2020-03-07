package com.alexdeww.rxniosockettcpclientlib.exceptions

class ConnectionError(cause: Throwable? = null) : RxConnectionException(cause = cause)
