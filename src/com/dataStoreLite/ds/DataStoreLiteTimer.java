package com.dataStoreLite.ds;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/*
Schedule a repeating task to remove the expired keys from DSCache
Shutdown the Executor Service after 30 times.

 */


public class DataStoreLiteTimer {

    static final Logger DSLog = (Logger) LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);
    static final int TIME_INTERVAL = 5;
    static final int SHUTDOWN_COUNTER = 30;
   protected  static void scheduleTask (){
       AtomicInteger counter = new AtomicInteger();
        ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor();
       scheduledExecutorService.scheduleAtFixedRate(() -> {
           DataStoreLite.removeExpiredKeys();
           counter.getAndIncrement();
           DSLog.trace(counter);
           if(counter.get() == SHUTDOWN_COUNTER) {
               scheduledExecutorService.shutdown();
           }

       },0,TIME_INTERVAL,TimeUnit.SECONDS);



    }
}


