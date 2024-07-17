package com.dongwo.data.tunnel.canal.utils;

import lombok.extern.slf4j.Slf4j;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zebin.xuzb 2012-9-20 下午3:47:47
 * @version 1.0.0
 */
@Slf4j
public class CanalNamedThreadFactory implements ThreadFactory {

    final private static String           DEFAULT_NAME             = "canal-tunnel-worker";
    final private String                  name;
    final private boolean                 daemon;
    final private ThreadGroup             group;
    final private AtomicInteger           threadNumber             = new AtomicInteger(0);
    public final static UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> {
        Throwable ta = e;
        log.error("canal tunnel exception from : {}", t.getName(), ta);
    };

    public CanalNamedThreadFactory(){
        this(DEFAULT_NAME, true);
    }

    public CanalNamedThreadFactory(String name){
        this(name, true);
    }

    public CanalNamedThreadFactory(String name, boolean daemon){
        this.name = name;
        this.daemon = daemon;
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, name + "-" + threadNumber.getAndIncrement(), 0);
        t.setDaemon(daemon);
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }

        t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return t;
    }

}
