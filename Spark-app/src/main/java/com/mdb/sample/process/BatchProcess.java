package com.mdb.sample.process;

import java.io.Serializable;
import java.util.Map;

@FunctionalInterface
public interface BatchProcess extends Serializable {
    void executeBatchProcess(Map<String, Object> dependencies) ;
}
