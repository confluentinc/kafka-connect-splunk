/*
 * Copyright 2017 Splunk, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splunk.kafka.connect;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reproduction: a connector that shows RUNNING but sends nothing after a Splunk outage.
 *
 * <p>This test documents the CURRENT behavior, so it passes while the bug is present. When the bug is fixed,
 * the assertions marked "BUG" in {@link #droppedBatchWedgesTheTask()} must be flipped to the fixed behavior
 * (the tracker total returns to 0, the committed offset advances and {@code put()} keeps accepting records).
 *
 * <p>Root cause: when a batch reaches {@code splunk.hec.max.retries}, {@code SplunkSinkTask.handleFailedBatches()}
 * logs "dropping EventBatch" and skips it, but the batch is neither committed nor removed from
 * {@link KafkaRecordTracker}. {@code removeAckedEventBatches()} clears offsets in order and stops at the first
 * uncommitted one, so every later offset on that partition stays counted even after Splunk accepts it. The
 * outstanding event count only grows until {@code splunk.hec.max.outstanding.events}, after which
 * {@code put()} throws a RetriableException on every call. Kafka Connect retries that forever, so the task
 * never moves to FAILED.
 *
 * <p>Settings mirror the affected connector with scaled-down numbers: max.batch.size=1, max.retries=3
 * (affected connector: 60) and max.outstanding.events=100 (affected connector: 10000).
 * "Outage" is {@link HecMock#failure} (batch.fail + onEventFailure, the same callbacks a LoadBalancer
 * "No channels are available" error produces). "Splunk is back" is {@link HecMock#success}.
 */
public class DroppedBatchHoleReproTest {
    private static final int MAX_OUTSTANDING = 100;

    private final UnitUtil uu = new UnitUtil(0);
    private final String topic = uu.configProfile.getTopics();
    private final TopicPartition tp = new TopicPartition(topic, 1);

    private Map<String, String> config(int maxRetries) {
        Map<String, String> c = uu.createTaskConfig();
        c.put(SplunkSinkConnectorConfig.RAW_CONF, "false");
        c.put(SplunkSinkConnectorConfig.ACK_CONF, "false");
        c.put(SplunkSinkConnectorConfig.MAX_BATCH_SIZE_CONF, "1");
        c.put(SplunkSinkConnectorConfig.MAX_OUTSTANDING_EVENTS_CONF, String.valueOf(MAX_OUTSTANDING));
        c.put(SplunkSinkConnectorConfig.MAX_RETRIES_CONF, String.valueOf(maxRetries));
        return c;
    }

    private List<SinkRecord> records(int fromOffset, int count) {
        List<SinkRecord> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long off = fromOffset + i;
            list.add(new SinkRecord(topic, 1, null, null, null, "event-" + off, off));
        }
        return list;
    }

    private void log(String step, KafkaRecordTracker tracker, HecMock hec) {
        System.out.printf("[repro] %-46s tracker.total=%-4d sentToHec=%-4d committedOffsets=%s%n",
                step, tracker.totalEvents(), hec.getBatches().size(), tracker.computeOffsets());
    }

    /** A dropped batch leaves a hole. Later events are delivered but never cleared, and the task wedges at the cap. */
    @Test
    public void droppedBatchWedgesTheTask() {
        SplunkSinkTask task = new SplunkSinkTask();
        HecMock hec = new HecMock(task);
        task.setHec(hec);
        task.start(config(3));
        task.open(Collections.singletonList(tp));
        KafkaRecordTracker tracker = task.getTracker();

        // 1) Splunk outage: every send fails. 5 records (offsets 0..4).
        hec.setSendReturnResult(HecMock.failure);
        task.put(records(0, 5));
        log("1 outage: 5 records, all sends fail", tracker, hec);
        Assert.assertEquals(5, tracker.totalEvents());

        // 2) Further put() cycles resend the failed batches. After more than max.retries they are dropped.
        for (int i = 0; i < 6; i++) {
            task.put(Collections.<SinkRecord>emptyList());
        }
        log("2 retries exhausted, batches dropped", tracker, hec);
        Assert.assertEquals("5 initial sends + 5 batches x 3 resends, then dropped", 20, hec.getBatches().size());
        // BUG: the dropped events are still counted. Fixed behavior: 0.
        Assert.assertEquals("dropped events are still counted", 5, tracker.totalEvents());

        // 3) Splunk is back. Send 20 NEW records (offsets 5..24); Splunk accepts every one.
        hec.setSendReturnResult(HecMock.success);
        int sentBefore = hec.getBatches().size();
        for (int off = 5; off < 25; off++) {
            task.put(records(off, 1));
        }
        log("3 Splunk back: 20 new records accepted", tracker, hec);
        Assert.assertEquals("Splunk accepted all 20", 20, hec.getBatches().size() - sentBefore);
        // BUG: none of them is cleared. Fixed behavior: 0.
        Assert.assertEquals("...but none of them is cleared from the tracker", 25, tracker.totalEvents());
        // BUG: the committed offset cannot advance past the hole. Fixed behavior: offset 25.
        Assert.assertTrue("committed offset cannot advance past the hole", tracker.computeOffsets().isEmpty());

        // 4) Keep producing until the outstanding cap is reached.
        int off = 25;
        boolean capReached = false;
        while (off < 1000) {
            try {
                task.put(records(off++, 1));
            } catch (RetriableException e) {
                capReached = true;
                System.out.println("[repro] cap reached at offset " + off + ": " + e.getMessage());
                break;
            }
        }
        log("4 cap reached", tracker, hec);
        // BUG: the cap is reached even though Splunk accepted everything. Fixed behavior: never reached.
        Assert.assertTrue(capReached);
        Assert.assertTrue(tracker.totalEvents() >= MAX_OUTSTANDING);

        // 5) Splunk is healthy (success mode) but put() now refuses everything.
        int sentAtCap = hec.getBatches().size();
        for (int i = 0; i < 5; i++) {
            try {
                task.put(records(off++, 1));
                Assert.fail("put() should keep throwing RetriableException");
            } catch (RetriableException expected) {
                // Kafka Connect retries this forever, so the task stays RUNNING and never goes FAILED.
            }
        }
        log("5 five more put() calls with Splunk healthy", tracker, hec);
        Assert.assertEquals("nothing was sent to a healthy Splunk", sentAtCap, hec.getBatches().size());
        Assert.assertTrue(tracker.computeOffsets().isEmpty());
    }

    /** Control: the same outage with max.retries=-1 (the Cloud default). No drop, no hole, the task recovers by itself. */
    @Test
    public void unlimitedRetriesRecoverOnceSplunkIsBack() {
        SplunkSinkTask task = new SplunkSinkTask();
        HecMock hec = new HecMock(task);
        task.setHec(hec);
        task.start(config(-1));
        task.open(Collections.singletonList(tp));
        KafkaRecordTracker tracker = task.getTracker();

        hec.setSendReturnResult(HecMock.failure);
        task.put(records(0, 5));
        for (int i = 0; i < 6; i++) {
            task.put(Collections.<SinkRecord>emptyList());
        }
        log("control 1 outage, retries unlimited", tracker, hec);
        Assert.assertEquals(5, tracker.totalEvents());

        hec.setSendReturnResult(HecMock.success);
        task.put(Collections.<SinkRecord>emptyList());   // resends the 5 failed batches, now accepted
        log("control 2 Splunk back, failed batches resent", tracker, hec);
        Assert.assertEquals("everything cleared", 0, tracker.totalEvents());
        Map<TopicPartition, OffsetAndMetadata> expected = new HashMap<>();
        expected.put(tp, new OffsetAndMetadata(5));
        Assert.assertEquals("committed offset advanced", expected, tracker.computeOffsets());

        for (int off = 5; off < 125; off++) {
            task.put(records(off, 1));                    // well past the cap of 100 outstanding events
        }
        log("control 3 120 more records flow normally", tracker, hec);
        Assert.assertEquals(0, tracker.totalEvents());
    }
}
