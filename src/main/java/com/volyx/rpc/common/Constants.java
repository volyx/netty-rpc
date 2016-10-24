package com.volyx.rpc.common;

import io.netty.util.CharsetUtil;

public final class Constants {

    public static int DEFAULT_THREAD_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    public static int RECONNECT_DELAY = 15;

    public static int MAX_RETRY_TIMES = 3;

    public static int CANCEL_WAITING_REQUEST_DELAY = 7;

    public static int CONNECT_TIMEOUT = 15000;

    public static int HEARTBEAT_PERIOD = 60;

    public static String REQUEST_BOUNDARY = new String(new byte[]{0}, CharsetUtil.UTF_8);
}
