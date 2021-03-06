package com.volyx.rpc.common;

import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;

import com.volyx.rpc.common.message.KeepAlive;

public class KeepAliveTimer {

    private final Collection<NettyRemote> remotes;
    private final Timer timer;    

    public KeepAliveTimer(Collection<NettyRemote> remotes, long keepalivePeriod) {
        this.remotes = remotes;
        this.timer = new Timer("KeepAliveTimer", true);

        if(keepalivePeriod > 0) {
            timer.scheduleAtFixedRate(new KeepaliveTimerTask(), 0, keepalivePeriod);
        }
    }

    public void stop() {
        timer.cancel();
    }

    private class KeepaliveTimerTask extends TimerTask {
        @Override
        public void run() {
            for (NettyRemote remote : remotes) {
                remote.getChannel().writeAndFlush(new KeepAlive());
            }
        }
    }

}
