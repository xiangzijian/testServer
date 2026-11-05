package com.slg.common.msg;

public final class ErrorCodes {

    private ErrorCodes() {
    }

    public static final int UNKNOWN_MSG = 1;
    public static final int BACKEND_UNAVAILABLE = 2;
    public static final int BAD_REQUEST = 3;
    public static final int SERVER_ERROR = 4;
    public static final int NO_HANDLER = 5;
}
