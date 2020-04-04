package com.mdb.sample.utils;

import com.mdb.sample.process.impl.SurveyProcessImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ProcessUtil {
    // this process map holds process implementations.
    private static Map<Integer, Consumer> processMap = new HashMap<>();

    static {
        processMap.put(1, (Consumer<Map>)new SurveyProcessImpl()::executeBatchProcess);
    }

    /**
     * invokeProcess API allow to invoke corresponding process with dependencies.
     * @param processId process id to be invoked
     * @param dependencies dependencies of the invoking process
     */
    public static void invokeProcess(Integer processId, Map<String, Object> dependencies) {
        processMap.get(processId).accept(dependencies);
    }

}
