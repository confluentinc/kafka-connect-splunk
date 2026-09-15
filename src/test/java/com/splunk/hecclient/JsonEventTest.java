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
package com.splunk.hecclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonEventTest {
    static final ObjectMapper jsonMapper = new ObjectMapper();

    // Sensitive-log regression: when Jackson fails to serialize the record value, the resulting
    // exception is built over that value (its message/cause chain can carry it), so it must not be
    // handed to the logger. The canary below stands in for the record value; it must never appear
    // in the captured log output.
    private static final String RECORD_VALUE_CANARY = "S3cr3t-Splunk-Record-Value-CANARY-9f83a1";

    // A record value whose Jackson serialization fails with the canary carried on the exception.
    public static final class UnserializableRecordValue {
        public String getSecret() {
            throw new RuntimeException(RECORD_VALUE_CANARY);
        }
    }

    @Test
    public void getBytesDoesNotLogRecordValueOnSerializationFailure() {
        Event event = new JsonEvent(new UnserializableRecordValue(), null);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintStream cap = new PrintStream(buf, true);
        PrintStream origErr = System.err;
        PrintStream origOut = System.out;
        System.setErr(cap);
        System.setOut(cap);
        try {
            event.getBytes();
            Assert.fail("expected HecException from failed serialization");
        } catch (HecException expected) {
            // expected: serialization of the record value failed
        } finally {
            System.setErr(origErr);
            System.setOut(origOut);
        }
        String logged = buf.toString();
        Assert.assertTrue("the ERROR log line should still fire", logged.contains("Invalid json event"));
        Assert.assertFalse("record value must not reach the logs", logged.contains(RECORD_VALUE_CANARY));
    }

    @Test
    public void toStringDoesNotLogRecordValueOnSerializationFailure() {
        Event event = new JsonEvent(new UnserializableRecordValue(), null);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintStream cap = new PrintStream(buf, true);
        PrintStream origErr = System.err;
        PrintStream origOut = System.out;
        System.setErr(cap);
        System.setOut(cap);
        try {
            event.toString();
            Assert.fail("expected HecException from failed serialization");
        } catch (HecException expected) {
            // expected: serialization of the record value failed
        } finally {
            System.setErr(origErr);
            System.setOut(origOut);
        }
        String logged = buf.toString();
        Assert.assertTrue("the ERROR log line should still fire", logged.contains("failed to json serlized JsonEvent"));
        Assert.assertFalse("record value must not reach the logs", logged.contains(RECORD_VALUE_CANARY));
    }

    @Test
    public void createValidJsonEvent() {
        String data = "this is splunk event";

        // without tied object
        Event event = new JsonEvent(data, null);
        Assert.assertEquals(data, event.getEvent());
        Assert.assertEquals(null, event.getTied());

        // with tied object
        String tied = "i love you";
        event = new JsonEvent(data, tied);

        Assert.assertEquals(tied, event.getTied());
        Assert.assertEquals(data, event.getEvent());
    }

    @Test(expected = HecException.class)
    public void createInvalidJsonEventWithNullData() {
        Event event = new JsonEvent(null, null);
    }

    @Test(expected = HecException.class)
    public void createInvalidJsonEventWithEmptyString() {
        Event event = new JsonEvent("", null);
    }

    @Test
    public void addFields() {
        Event event = new JsonEvent("this is splunk event", null);

        // null extra fields
        event.addFields(null);
        Assert.assertNull(event.getFields());

        // empty extra fields
        Map<String, String> fields = new HashMap<>();
        event.addFields(fields);
        Assert.assertNull(event.getFields());

        // one item
        fields.put("ni", "hao");
        event.addFields(fields);
        Map<String, String> fieldsGot = event.getFields();
        Assert.assertNotNull(fieldsGot);
        Assert.assertEquals(false, fieldsGot.isEmpty());
        Assert.assertEquals(1, fieldsGot.size());
        Assert.assertEquals("hao", fieldsGot.get("ni"));

        // put another one
        fields.put("hello", "world");
        event.addFields(fields);
        fieldsGot = event.getFields();
        Assert.assertNotNull(fieldsGot);
        Assert.assertEquals(false, fieldsGot.isEmpty());
        Assert.assertEquals(2, fieldsGot.size());
        Assert.assertEquals("hao", fieldsGot.get("ni"));
        Assert.assertEquals("world", fieldsGot.get("hello"));
    }

    @Test
    public void toStr() {
        SerialAndDeserial sad = new SerialAndDeserial() {
            @Override
            public Event serializeAndDeserialize(Event event) {
                String stringed = event.toString();
                Assert.assertNotNull(stringed);

                Event deserilized;
                try {
                    deserilized = jsonMapper.readValue(stringed, JsonEvent.class);
                } catch (IOException ex) {
                    Assert.assertFalse("expect no exception but got exception", true);
                    throw new HecException("failed to parse JsonEvent", ex);
                }
                return deserilized;
            }
        };
        serialize(sad);
    }

    @Test
    public void getBytes() {
        SerialAndDeserial sad = new SerialAndDeserial() {
            @Override
            public Event serializeAndDeserialize(Event event) {
                byte[] bytes = event.getBytes();
                Assert.assertNotNull(bytes);

                Event deserilized;
                try {
                    deserilized = jsonMapper.readValue(bytes, JsonEvent.class);
                } catch (IOException ex) {
                    Assert.assertFalse("expect no exception but got exception", false);
                    throw new HecException("failed to parse JsonEvent", ex);
                }
                return deserilized;
            }
        };
        serialize(sad);
    }

    @Test
    public void getInputStream() {
        Event event = new JsonEvent("hello", "world");
        InputStream stream = event.getInputStream();
        byte[] data = new byte[1024];
        int siz = UnitUtil.read(stream, data);

        Event eventGot;
        try {
            eventGot = jsonMapper.readValue(data, 0, siz, JsonEvent.class);
        } catch (IOException ex) {
            Assert.assertTrue("failed to deserialize from bytes", false);
            throw new HecException("failed to deserialize from bytes", ex);
        }
        Assert.assertEquals("hello", eventGot.getEvent());
    }

    @Test
    public void getterSetter() {
        Event event = new JsonEvent("hello", "world");
        Assert.assertEquals("hello", event.getEvent());
        Assert.assertEquals("world", event.getTied());

        Assert.assertNull(event.getIndex());
        event.setIndex("main");
        Assert.assertEquals("main", event.getIndex());

        Assert.assertNull(event.getSource());
        event.setSource("source");
        Assert.assertEquals("source", event.getSource());

        Assert.assertNull(event.getSourcetype());
        event.setSourcetype("sourcetype");
        Assert.assertEquals("sourcetype", event.getSourcetype());

        Assert.assertNull(event.getHost());
        event.setHost("localhost");
        Assert.assertEquals("localhost", event.getHost());

        Assert.assertNull(event.getTime());
        event.setTime(1.0);
        Assert.assertEquals(new Double(1.0), event.getTime());

        event.setEvent("ni");
        Assert.assertEquals("ni", event.getEvent());

        event.setTied("hao");
        Assert.assertEquals("hao", event.getTied());

        Map<String, String> fields = new HashMap<>();
        fields.put("hello", "world");
        event.setFields(fields);
        Assert.assertEquals(fields, event.getFields());

        Map<String, String> moreFields = new HashMap<>();
        moreFields.put("ni", "hao");
        event.addFields(moreFields);
        Map<String, String> got = event.getFields();
        Assert.assertNotNull(got);
        Assert.assertEquals(2, got.size());
        Assert.assertEquals("world", got.get("hello"));
        Assert.assertEquals("hao", got.get("ni"));
    }

    private interface SerialAndDeserial {
        Event serializeAndDeserialize(final Event event);
    }

    private void serialize(SerialAndDeserial sad) {
        List<Object> eventDataObjs = new ArrayList<>();
        // String object
        eventDataObjs.add("this is splunk event");

        Map<String, String> m = new HashMap<>();
        m.put("hello", "world");

        // Json object
        eventDataObjs.add(m);

        for (Object eventData: eventDataObjs) {
            doSerialize(eventData, sad);
        }
    }

    private void doSerialize(Object data, SerialAndDeserial sad) {
        String tied = "tied";
        Event event = new JsonEvent(data, tied);

        Map<String, String> fields = new HashMap<>();
        fields.put("ni", "hao");
        event.addFields(fields);
        event.setHost("localhost");
        event.setIndex("main");
        event.setSource("test-source");
        event.setSourcetype("test-sourcetype");
        event.setTime(100000000.0);

        for (int i = 0; i < 2; i++) {
            Event deserialized = sad.serializeAndDeserialize(event);

            Assert.assertEquals(data, deserialized.getEvent());
            Assert.assertNull(deserialized.getTied()); // we ignore tied when serialize Event
            Assert.assertEquals("localhost", deserialized.getHost());
            Assert.assertEquals("main", deserialized.getIndex());
            Assert.assertEquals("test-source", deserialized.getSource());
            Assert.assertEquals("test-sourcetype", deserialized.getSourcetype());
            Assert.assertEquals(event.getTime(), deserialized.getTime());

            Map<String, String> fieldsGot = deserialized.getFields();
            Assert.assertEquals("hao", fieldsGot.get("ni"));
        }
    }
}