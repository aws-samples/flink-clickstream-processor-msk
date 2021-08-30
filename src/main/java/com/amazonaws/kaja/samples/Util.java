package com.amazonaws.kaja.samples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

class Util {
    //private static final Logger LOG = LoggerFactory.getLogger(ClickstreamProcessor.class);

    static String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

}
