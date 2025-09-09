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

    String orderId = "ORD-" + System.currentTimeMillis();
    String json = "{\"orderId\":\"" + orderId + "\",\"customer\":\"Alice\",\"total\":12.34}";
    connect.kafka().produce(TEST_TOPIC, json);

    String searchQuery = createSearchQuery(TEST_INDEX, TEST_SOURCE_TYPE);
    await().atMost(CONSUME_MAX_DURATION_MS, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertTrue(searchFromSplunkIndex(searchQuery).contains(orderId)));

    String notPresent = "Bob";
    assertFalse(searchFromSplunkIndex(searchQuery).contains(notPresent));
  }
}
