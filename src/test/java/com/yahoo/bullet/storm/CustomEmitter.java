/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import lombok.Getter;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@Getter
public class CustomEmitter implements ISpoutOutputCollector {
    @Getter
    public static class Triplet {
        private String streamId;
        private List<Object> tuple;
        private Object messageId;

        public Triplet(String streamId, List<Object> tuple, Object messageId) {
            this.streamId = streamId;
            this.tuple = tuple != null ? tuple : new ArrayList<>();
            this.messageId = messageId;
        }

        @Override
        public String toString() {
            return "<Stream: " + streamId + ", " + "<Tuple: " +
                    tuple.stream().reduce((a, b) -> a.toString() + "," + b.toString()).orElse("None") + ">, " +
                    "MessageId: " + messageId == null ? "None" : messageId.toString() + ">";
        }
    }

    private List<Triplet> emitted = new ArrayList<>();

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        emitted.add(new Triplet(streamId, tuple, messageId));
        return Collections.singletonList(emitted.size() - 1);
    }

    public Stream<Triplet> getAllEmitted() {
        return emitted.stream();
    }

    public Stream<Triplet> getAllEmittedTo(String stream) {
        return getAllEmitted().filter(t -> stream.equals(t.getStreamId()));
    }

    public Stream<List<Object>> getTuplesEmittedTo(String stream) {
        return getAllEmittedTo(stream).map(Triplet::getTuple);
    }

    public boolean wasTupleEmitted(List<Object> tuple) {
        return getAllEmitted().map(Triplet::getTuple).anyMatch(t -> t.equals(tuple));
    }

    public boolean wasTupleEmitted(List<Object> tuple, int times) {
        return getAllEmitted().map(Triplet::getTuple).filter(t -> t.equals(tuple)).count() == times;
    }

    public boolean wasTupleEmittedTo(List<Object> tuple, String stream) {
        return getTuplesEmittedTo(stream).anyMatch(t -> t.equals(tuple));
    }

    public boolean wasTupleEmittedTo(List<Object> tuple, String stream, int times) {
        return getTuplesEmittedTo(stream).filter(t -> t.equals(tuple)).count() == times;
    }

    public boolean wasTupleEmitted(Tuple tuple) {
        return wasTupleEmitted(tuple.getValues());
    }

    public boolean wasTupleEmitted(Tuple tuple, int times) {
        return wasTupleEmitted(tuple.getValues(), times);
    }

    public boolean wasTupleEmittedTo(Tuple tuple, String stream) {
        return wasTupleEmittedTo(tuple.getValues(), stream);
    }

    public boolean wasTupleEmittedTo(Tuple tuple, String stream, int times) {
        return wasTupleEmittedTo(tuple.getValues(), stream, times);
    }

    public boolean wasNthEmitted(List<Object> tuple, int n) {
        return emitted.size() >= n && emitted.get(n - 1).getTuple().equals(tuple);
    }

    public boolean wasNthEmitted(Tuple tuple, int n) {
        return wasNthEmitted(tuple.getValues(), n);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        throw new UnsupportedOperationException("Emit Direct not supported");
    }

    @Override
    public long getPendingCount() {
        throw new UnsupportedOperationException("Get Pending Count not supported");
    }

    @Override
    public void reportError(Throwable error) {
        throw new UnsupportedOperationException("Report Error not supported");
    }
}


