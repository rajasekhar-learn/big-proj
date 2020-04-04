package com.mdb.sample.process;

import java.util.Map;

@FunctionalInterface
public interface BatchProcess {
    void executeBatchProcess(Map<String, Object> dependencies) ;
}
