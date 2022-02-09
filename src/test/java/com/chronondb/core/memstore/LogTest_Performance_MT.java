package com.chronondb.core.memstore;

import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

public class LogTest_Performance_MT {

    @Test
    public void perfMTTest() throws InterruptedException, ExecutionException {

        LogMTRunner.runLogSimplePerformanceMTTest(1, 10*1000);
        LogMTRunner.runLogSimplePerformanceMTTest(5, 10*1000);
        LogMTRunner.runLogSimplePerformanceMTTest(10, 10*1000);
        LogMTRunner.runLogSimplePerformanceMTTest(20, 10*1000);
        LogMTRunner.runLogSimplePerformanceMTTest(50, 10*1000);
        LogMTRunner.runLogSimplePerformanceMTTest(75, 10*1000);
        LogMTRunner.runLogSimplePerformanceMTTest(100, 10*1000);
        LogMTRunner.runLogSimplePerformanceMTTest(150, 10*1000);
        LogMTRunner.runLogSimplePerformanceMTTest(200, 10*1000);


    }

}
