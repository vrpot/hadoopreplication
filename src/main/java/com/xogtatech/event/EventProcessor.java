package com.xogtatech.event;

import org.apache.hadoop.hdfs.inotify.Event;

import java.io.IOException;
import java.util.List;

public interface EventProcessor {
    void processEvents(List<Event> events) throws IOException;
}
