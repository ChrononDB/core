package com.chronondb.core.memstore;

import com.chronondb.core.properties.DefaultLogProperties;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogMTRunner {

    static void runLogSimplePerformanceMTTest(int threads, int chunk) throws ExecutionException, InterruptedException {
        Log<Long, Integer> log = new Log<Long, Integer>(new DefaultLogProperties().setLockThresholdMs(400)); //GC

        ExecutorService executorService = Executors.newFixedThreadPool(threads);

        var tasks = new ArrayList<Callable<ExecutionStatus>>(threads);

        for (int i = 0; i < threads; i++) {
            tasks.add(new SimplePerformanceTestRunner(i, log).setChunkSize(chunk));
        }

        var results = executorService.invokeAll(tasks);

        var globalStat = new ExecutionStatus();

        for (var result : results) {
            var stat = result.get();
            if (stat.crashed) throw new ExecutionException("One of the threads crashed with " + stat.crashMessage, stat.crashException);
            globalStat.append(stat);
        }

        executorService.shutdown();
        System.out.println("PERFORMANCE & STABILITY REPORT -- Threads: "+ threads + ", chunk size is "+ chunk + " records");
        globalStat.print();

    }
}
