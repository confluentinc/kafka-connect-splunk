package com.splunk.kafka.connect.integration;

import com.splunk.kafka.connect.SplunkSinkConnector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.splunk.kafka.connect.SplunkSinkConnectorConfig.*;
import static org.apache.kafka.connect.runtime.ConnectorConfig.*;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;

public abstract class BaseSplunkSinkIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseSplunkSinkIT.class);

  protected static final String SPLUNK_PASSWORD = "password";
  protected static final String SPLUNK_USERNAME = "admin";
  protected static final String SPLUNK_HEC_TOKEN = "itest-token";
  protected static final int TASKS_MAX = 1;
  protected static final String TEST_INDEX = "main";
  protected static final String TEST_SOURCE_TYPE = "my_sourcetype";
  protected static final String INDEXER_IP = "ind";
  private static final String SPLUNK_BASE_IMAGE = "splunk/splunk";
  private static final String SPLUNK_VERSION = "8.1";
  private static final String AUTHORIZATION = "Authorization";
  private static final String BASIC_ = "Basic ";
  private static final String UTF8 = "UTF-8";
  private static final Map<String, String> splunkEnv = ImmutableMap.<String, String>builder()
      .put("SPLUNK_START_ARGS", "--accept-license")
      .put("SPLUNK_PASSWORD", SPLUNK_PASSWORD)
      .put("SPLUNK_USERNAME", SPLUNK_USERNAME)
      .put("SPLUNK_HEC_DISABLED", "0")
      .put("SPLUNK_HEC_TOKEN", SPLUNK_HEC_TOKEN)
      .build();

  @ClassRule
  public static final Network network = Network.newNetwork();

  @ClassRule
  public static final GenericContainer<?> splunk = new GenericContainer<>(
      DockerImageName.parse(SPLUNK_BASE_IMAGE + ":" + SPLUNK_VERSION))
      .withEnv(splunkEnv)
      .withNetwork(network)
      .withNetworkAliases(INDEXER_IP)
      .withExposedPorts(8000, 8089, 8088);

  protected static String splunkHecUrl;
  protected static String splunkBaseUrl;

  @BeforeClass
  public static void setupHec() throws Exception {
    splunkHecUrl = "https://" + splunk.getHost() + ":" + splunk.getMappedPort(8088);
    splunkBaseUrl = "https://" + splunk.getHost() + ":" + splunk.getMappedPort(8089);

    HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
    TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[]{
        new X509TrustManager() {
          public X509Certificate[] getAcceptedIssuers() {
            return null;
          }

          public void checkClientTrusted(X509Certificate[] certs, String authType) {
          }

          public void checkServerTrusted(X509Certificate[] certs, String authType) {
          }
        }
    };
    SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("SSL");
    sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());

    Thread.sleep(10000);
  }

  @Before
  public void setupConnect() {
    startConnect();
  }

  @After
  public void cleanupConnect() {
    stopConnect();
  }

  protected Map<String, String> getBaseConnectorProps(String connectorName, String topic) {
    return getBaseConnectorProps(connectorName, topic, TEST_INDEX, TEST_SOURCE_TYPE);
  }

  protected Map<String, String> getBaseConnectorProps(String connectorName, String topic, String index, String sourceType) {
    Map<String, String> props = new HashMap<>();
    props.put(NAME_CONFIG, connectorName);
    props.put(CONNECTOR_CLASS_CONFIG, SplunkSinkConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));

    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put("value.converter.schemas.enable", "false");

    props.put(TOPICS_CONFIG, topic);
    props.put(INDEX_CONF, index);
    props.put(URI_CONF, splunkHecUrl);
    props.put(TOKEN_CONF, SPLUNK_HEC_TOKEN);
    props.put(SSL_VALIDATE_CERTIFICATES_CONF, "false");
    props.put(SOURCETYPE_CONF, sourceType);
    props.put(MAX_BATCH_SIZE_CONF, "1");

    return props;
  }

  protected String createSearchQuery(String index, String sourceType) {
    return "index=" + index + " sourcetype=" + sourceType;
  }

  protected String searchFromSplunkIndex(String searchQuery) throws Exception {
    String jobId = submitSearchJob(searchQuery);
    if (jobId == null || !waitForSearchCompletion(jobId)) {
      return null;
    }
    return getSearchResults(jobId);
  }

  private String submitSearchJob(String searchQuery) throws Exception {
    URL url = new URL(splunkBaseUrl + "/services/search/jobs?output_mode=json");
    HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setConnectTimeout(10000);
    conn.setReadTimeout(10000);
    conn.setRequestProperty(AUTHORIZATION, BASIC_ +
        Base64.getEncoder().encodeToString((SPLUNK_USERNAME + ":" + SPLUNK_PASSWORD).getBytes()));
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

    String formData = "search=" + URLEncoder.encode("search " + searchQuery, UTF8) +
        "&earliest_time=" + URLEncoder.encode("-15m@m", UTF8) +
        "&latest_time=" + URLEncoder.encode("now", UTF8);
    conn.getOutputStream().write(formData.getBytes());
    conn.getOutputStream().flush();

    if (conn.getResponseCode() != 201) {
      log.error("Failed to submit search job: HTTP {}", conn.getResponseCode());
      return null;
    }

    Scanner sc = new Scanner(conn.getInputStream()).useDelimiter("\\A");
    String response = sc.hasNext() ? sc.next() : "";
    sc.close();
    Pattern pattern = Pattern.compile("\"sid\"\\s*:\\s*\"([^\"]+)\"");
    Matcher matcher = pattern.matcher(response);
    if (matcher.find()) {
      return matcher.group(1);
    }
    log.error("Failed to submit search job: HTTP {}", conn.getResponseCode());
    return null;
  }

  private boolean waitForSearchCompletion(String jobId) throws Exception {
    for (int i = 0; i < 60; i++) {
      URL url = new URL(splunkBaseUrl + "/services/search/jobs/" + jobId + "?output_mode=json");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(10000);
      conn.setReadTimeout(10000);
      conn.setRequestProperty(AUTHORIZATION, BASIC_ +
          Base64.getEncoder().encodeToString((SPLUNK_USERNAME + ":" + SPLUNK_PASSWORD).getBytes()));

      if (conn.getResponseCode() == 200) {
        Scanner sc = new Scanner(conn.getInputStream()).useDelimiter("\\A");
        String response = sc.hasNext() ? sc.next() : "";
        sc.close();

        if (response.contains("\"isDone\":true")) {
          return true;
        }
      }

      Thread.sleep(1000);
    }
    log.error("Search job {} did not complete in time", jobId);
    return false;
  }

  private String getSearchResults(String jobId) throws Exception {
    URL url = new URL(splunkBaseUrl + "/services/search/jobs/" + jobId + "/results?output_mode=json&count=10");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(10000);
    conn.setReadTimeout(10000);
    conn.setRequestProperty(AUTHORIZATION, BASIC_ +
        java.util.Base64.getEncoder().encodeToString((SPLUNK_USERNAME + ":" + SPLUNK_PASSWORD).getBytes()));

    if (conn.getResponseCode() != 200) {
      log.error("Failed to get search results: HTTP {}", conn.getResponseCode());
      return null;
    }

    java.util.Scanner sc = new java.util.Scanner(conn.getInputStream()).useDelimiter("\\A");
    String response = sc.hasNext() ? sc.next() : "";
    sc.close();

    return response;
  }
}


