/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storm;

import lombok.Getter;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class CustomCollector implements IOutputCollector {
    @Getter
    public static class Triplet {
        private String streamId;
        private Collection<Tuple> anchors;
        private List<Object> tuple;

        public Triplet(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            this.streamId = streamId;
            this.anchors = anchors != null ? anchors : new ArrayList<>();
            this.tuple = tuple != null ? tuple : new ArrayList<>();
        }

        @Override
        public String toString() {
            return "<Stream: " + streamId + ", " + "<Anchors: " +
                    anchors.stream().map(Object::toString).reduce((a, b) -> a + "," + b).orElse("None") +
                    ">, <" + tuple.stream().reduce((a, b) -> a.toString() + "," + b.toString()).orElse("None") + ">";
        }
    }

    private List<Triplet> emitted = new ArrayList<>();
    private List<Tuple> acked = new ArrayList<>();
    private List<Tuple> failed = new ArrayList<>();

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        emitted.add(new Triplet(streamId, anchors, tuple));
        return Collections.singletonList(emitted.size() - 1);
    }

    @Override
    public void ack(Tuple input) {
        acked.add(input);
    }

    @Override
    public void fail(Tuple input) {
        failed.add(input);
    }

    @Override
    public void resetTimeout(Tuple input) {
        throw new UnsupportedOperationException("Reset Timeout not supported");
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        throw new UnsupportedOperationException("Emit Direct not supported");
    }

    @Override
    public void reportError(Throwable error) {
        throw new UnsupportedOperationException("Report Error not supported");
    }

    /* Custom methods for helping with tests */

    public Stream<Triplet> getAllEmitted() {
        return emitted.stream();
    }

    public Stream<Triplet> getAllEmittedTo(String stream) {
        return getAllEmitted().filter(t -> stream.equals(t.getStreamId()));
    }

    public Stream<Tuple> getAcked() {
        return acked.stream();
    }

    public Stream<Tuple> getFailed() {
        return failed.stream();
    }

    public Stream<List<Object>> getTuplesEmitted() {
        return getAllEmitted().map(Triplet::getTuple);
    }

    public Stream<List<Object>> getTuplesEmittedTo(String stream) {
        return getAllEmittedTo(stream).map(Triplet::getTuple);
    }

    public Optional<List<Object>> getNthTupleEmittedTo(String stream, int n) {
        List<List<Object>> tuples = getAllEmittedTo(stream).map(Triplet::getTuple).collect(Collectors.toList());
        return tuples.size() >= n - 1 ? Optional.of(tuples.get(n - 1)) : Optional.ofNullable(null);
    }

    public Optional<Object> getMthElementFromNthTupleEmittedTo(String stream, int n, int m) {
        List<List<Object>> tuples = getAllEmittedTo(stream).map(Triplet::getTuple).collect(Collectors.toList());
        List<Object> tuple = tuples.size() >= n - 1 ? tuples.get(n - 1) : null;
        if (tuple != null) {
            try {
                return Optional.ofNullable(tuple.get(m));
            } catch (IndexOutOfBoundsException e) {
            }
        }
        return Optional.empty();
    }

    public boolean wasTupleEmitted(Tuple tuple) {
        return wasTupleEmitted(tuple.getValues());
    }

    public boolean wasTupleEmitted(List<Object> tuple) {
        return getAllEmitted().map(Triplet::getTuple).anyMatch(t -> t.equals(tuple));
    }

    public boolean wasTupleEmitted(Tuple tuple, int times) {
        return wasTupleEmitted(tuple.getValues(), times);
    }

    public boolean wasTupleEmitted(List<Object> tuple, int times) {
        return getAllEmitted().map(Triplet::getTuple).filter(t -> t.equals(tuple)).count() == times;
    }

    public boolean wasTupleEmittedTo(Tuple tuple, String stream) {
        return wasTupleEmittedTo(tuple.getValues(), stream);
    }

    public boolean wasTupleEmittedTo(List<Object> tuple, String stream) {
        return getTuplesEmittedTo(stream).anyMatch(t -> t.equals(tuple));
    }

    public boolean wasTupleEmittedTo(Tuple tuple, String stream, int times) {
        return wasTupleEmittedTo(tuple.getValues(), stream, times);
    }

    public boolean wasTupleEmittedTo(List<Object> tuple, String stream, int times) {
        return getTuplesEmittedTo(stream).filter(t -> t.equals(tuple)).count() == times;
    }

    public boolean wasAcked(List<Object> input) {
        return getAcked().anyMatch(t -> t.getValues().equals(input));
    }

    public boolean wasFailed(List<Object> input) {
        return getFailed().anyMatch(t -> t.getValues().equals(input));
    }

    public boolean wasAcked(Tuple input) {
        return wasAcked(input.getValues());
    }

    public boolean wasFailed(Tuple input) {
        return wasFailed(input.getValues());
    }

    public boolean wasNthEmitted(List<Object> tuple, int n) {
        return emitted.size() >= n && emitted.get(n - 1).getTuple().equals(tuple);
    }

    public boolean wasNthEmitted(Tuple tuple, int n) {
        return wasNthEmitted(tuple.getValues(), n);
    }

    public boolean wasNthAcked(Tuple input, int n) {
        return acked.size() >= n && acked.get(n - 1).equals(input);
    }

    public boolean wasNthFailed(Tuple input, int n) {
        return failed.size() >= n && failed.get(n - 1).equals(input);
    }

    public int getAckedCount() {
        return acked.size();
    }

    public int getFailedCount() {
        return failed.size();
    }

    public int getEmittedCount() {
        return emitted.size();
    }
}


