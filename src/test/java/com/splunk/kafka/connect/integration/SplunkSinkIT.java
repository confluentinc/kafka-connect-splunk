package com.splunk.kafka.connect.integration;

import org.apache.kafka.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class SplunkSinkIT extends BaseSplunkSinkIT {
  private static final String CONNECTOR_NAME = "splunk-sink-integration-test";

  // This is the first integration test, Please use BaseSplunkSinkIT methods for new tests
  // Feel free to add/update variables and methods in BaseSplunkSinkIT if needed
  @Test
  public void testSimpleIngestion() throws Exception {
    connect.kafka().createTopic(TEST_TOPIC);

    connect.configureConnector(CONNECTOR_NAME, getBaseConnectorProps(CONNECTOR_NAME, TEST_TOPIC));
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    String[] customers = {"Alice", "Bob", "Carol", "Dave", "Eve"};
    String[] orderIds = new String[customers.length];
    for (int i = 0; i < customers.length; i++) {
      orderIds[i] = "ORD-" + (System.currentTimeMillis() + i);
      String json = "{\"orderId\":\"" + orderIds[i] + "\",\"customer\":\"" + customers[i] + "\",\"total\":" + (10.0 + i) + "}";
      connect.kafka().produce(TEST_TOPIC, json);
    }

    for (String orderId : orderIds) {
      String exactQuery = createExactMatchQuery(TEST_INDEX, TEST_SOURCE_TYPE, "orderId", orderId);
      await().atMost(CONSUME_MAX_DURATION_MS, TimeUnit.MILLISECONDS)
          .untilAsserted(() -> assertTrue(searchFromSplunkIndex(exactQuery).contains(orderId)));
    }

    String notPresent = "THIS_STRING_SHOULD_NOT_EXIST_" + System.nanoTime();
    String negativeQuery = createExactMatchQuery(TEST_INDEX, TEST_SOURCE_TYPE, "orderId", notPresent);
    assertFalse(searchFromSplunkIndex(negativeQuery).contains(notPresent));
  }
}
