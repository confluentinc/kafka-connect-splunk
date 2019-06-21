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

import com.splunk.hecclient.HecConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public final class SplunkSinkConnectorConfig extends AbstractConfig {
    // General
    static final String INDEX = "index";
    static final String SOURCE = "source";
    static final String SOURCETYPE = "sourcetype";
    // Required Parameters
    static final String URI_CONF = "splunk.hec.uri";
    static final String TOKEN_CONF = "splunk.hec.token";
    // General Parameters
    static final String INDEX_CONF = "splunk.indexes";
    static final String SOURCE_CONF = "splunk.sources";
    static final String SOURCETYPE_CONF = "splunk.sourcetypes";

    static final String TOTAL_HEC_CHANNEL_CONF = "splunk.hec.total.channels";
    static final String MAX_HTTP_CONNECTION_PER_CHANNEL_CONF = "splunk.hec.max.http.connection.per.channel";
    static final String MAX_BATCH_SIZE_CONF = "splunk.hec.max.batch.size"; // record count
    static final String HTTP_KEEPALIVE_CONF = "splunk.hec.http.keepalive";
    static final String HEC_THREDS_CONF = "splunk.hec.threads";
    static final String SOCKET_TIMEOUT_CONF = "splunk.hec.socket.timeout"; // seconds
    static final String SSL_VALIDATE_CERTIFICATES_CONF = "splunk.hec.ssl.validate.certs";
    // Acknowledgement Parameters
    // Use Ack
    static final String ACK_CONF = "splunk.hec.ack.enabled";
    static final String ACK_POLL_INTERVAL_CONF = "splunk.hec.ack.poll.interval"; // seconds
    static final String ACK_POLL_THREADS_CONF = "splunk.hec.ack.poll.threads";
    static final String EVENT_TIMEOUT_CONF = "splunk.hec.event.timeout"; // seconds
    static final String MAX_OUTSTANDING_EVENTS_CONF = "splunk.hec.max.outstanding.events";
    static final String MAX_RETRIES_CONF = "splunk.hec.max.retries";
    static final String HEC_BACKOFF_PRESSURE_THRESHOLD = "splunk.hec.backoff.threshhold.seconds";
    // Endpoint Parameters
    static final String RAW_CONF = "splunk.hec.raw";
    // /raw endpoint only
    static final String LINE_BREAKER_CONF = "splunk.hec.raw.line.breaker";
    // /event endpoint only
    static final String USE_RECORD_TIMESTAMP_CONF = "splunk.hec.use.record.timestamp";
    static final String ENRICHMENT_CONF = "splunk.hec.json.event.enrichment";
    static final String TRACK_DATA_CONF = "splunk.hec.track.data";
    static final String HEC_EVENT_FORMATTED_CONF = "splunk.hec.json.event.formatted";
    // Trust store
    static final String SSL_TRUSTSTORE_PATH_CONF = "splunk.hec.ssl.trust.store.path";
    static final String SSL_TRUSTSTORE_PASSWORD_CONF = "splunk.hec.ssl.trust.store.password";
    //Headers
    static final String HEADER_SUPPORT_CONF = "splunk.header.support";
    static final String HEADER_CUSTOM_CONF = "splunk.header.custom";
    static final String HEADER_INDEX_CONF = "splunk.header.index";
    static final String HEADER_SOURCE_CONF = "splunk.header.source";
    static final String HEADER_SOURCETYPE_CONF = "splunk.header.sourcetype";
    static final String HEADER_HOST_CONF = "splunk.header.host";

    // Kafka configuration description strings
    // Required Parameters
    static final String URI_DOC = "Splunk HEC URIs. Either a list of FQDNs or IPs of all Splunk indexers, separated " +
            "with a ``,``, or a load balancer. The connector load balances to indexers using round robin. Splunk " +
            "Connector round robins to this list of indexers: " +
            "``https://hec1.splunk.com:8088,https://hec2.splunk.com:8088,https://hec3.splunk.com:8088``\n";
    static final String TOKEN_DOC = "Splunk Http Event Collector (HEC) token.";
    // General Parameters
    static final String INDEX_DOC = "Splunk index names for Kafka topic data separated by a comma for multiple topics " +
            "to indexers. Example: \"prod-index1,prod-index2,prod-index3\"";
    static final String SOURCE_DOC = "Splunk event source metadata for Kafka topic data. The same configuration rules as " +
            "indexes apply. If unconfigured, the default source binds to the HEC token.";
    static final String SOURCETYPE_DOC = "Splunk event source type metadata for Kafka topic data. The same configuration " +
            "rules as indexes apply here. If unconfigured, the default source binds to the HEC token. Only configure " +
            "this when using the JSON Event endpoint (``splunk.hec.raw=false``).";
    static final String TOTAL_HEC_CHANNEL_DOC = "Total HEC Channels used to post events to Splunk. When enabling HEC ACK, "
            + "setting to the same or 2X number of indexers is generally good.";
    static final String MAX_HTTP_CONNECTION_PER_CHANNEL_DOC = "The maximum number of HTTP connections pooled for one " +
            "HEC Channel when posting events to Splunk.";
    static final String MAX_BATCH_SIZE_DOC = "The maximum batch size when posting events to Splunk. The size is the " +
            "actual number of Kafka records, not the byte size. By default, this is set to ``500``.";
    static final String HTTP_KEEPALIVE_DOC = "This setting enables or disables `HTTP connection keep-alive " +
            "<https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Keep-Alive>`_. By default, this is set to ``true``.";
    static final String HEC_THREADS_DOC = "Controls how many threads are spawned to do data injection via HEC in a single "
            + "connector task. By default, this is set to 1.";
    static final String SOCKET_TIMEOUT_DOC = "The maximum duration in seconds to read/write data to network before an " +
            "internal TCP Socket timeout occurs. By default, this is set to 60 seconds.";
    static final String SSL_VALIDATE_CERTIFICATES_DOC = "Enables or disables HTTPS certification validation. " +
            "By default, this is set to ``true``.";
    // Acknowledgement Parameters
    // Use Ack
    static final String ACK_DOC = "When set to ``true``, the connector polls event ACKs for POST events before " +
            "check-pointing the Kafka offsets. This setting enables guaranteed delivery and prevents data loss but may " +
            "result in lower overall throughput.";
    static final String ACK_POLL_INTERVAL_DOC = "Controls the event ACKs polling interval. This setting is only " +
            "applicable when ``splunk.hec.ack.enabled`` is set to ``true``.  By default, this setting is ``10`` seconds.";
    static final String ACK_POLL_THREADS_DOC = "Controls how many threads should be spawned to poll event ACKs. " +
            "This setting is used for performance tuning and is only applicable when ``splunk.hec.ack.enabled`` is set " +
            "to ``true``.  By default, this is set to ``2``.\n";
    static final String EVENT_TIMEOUT_DOC = "This setting determines how long the connector will wait for an event to be " +
            "acknowledged before timing out and attempting to resend the event. This setting is applicable when " +
            "``splunk.hec.ack.enabled`` is set to ``true``. By default, this is set to ``300`` seconds.";
    static final String MAX_OUTSTANDING_EVENTS_DOC = "The maximum amount of unacknowledged events kept in memory by the " +
            "connector. When the threshold is exceeded, a back pressure event is triggered to slow the collection of " +
            "events. By default, this threshold is set to ``1000000`` events.";
    static final String MAX_RETRIES_DOC = "The maximum number of retries for a failed batch before the task is killed. " +
            "When set to ``-1`` (the default) the connector retries indefinitely.";

    static final String HEC_BACKOFF_PRESSURE_THRESHOLD_DOC = "The amount of time the connector waits before attempting " +
            "to resend failed events to Splunk.";
    // Endpoint Parameters
    static final String RAW_DOC = "Enable this setting to ingest data using the ``/raw`` HEC endpoint instead of the " +
            "``/event`` HEC endpoint. By default, this setting is ``false`` and the ``/event`` HEC endpoint is used.";
    // /raw endpoint only
    static final String LINE_BREAKER_DOC = "This setting is used to specify a custom line breaker to help Splunk " +
            "separate events correctly. For example, you can specify ``#####`` as a special line breaker and Splunk will " +
            "split events on those characters. This is only applicable when ``splunk.hec.raw`` is set to ``true``.";
    // /event endpoint only
    static final String USE_RECORD_TIMESTAMP_DOC = "When set to ``true``, the timestamp is retrieved from the Kafka " +
            "record and passed to Splunk as a HEC meta-data override. This indexes events in Splunk with the record " +
            "timestamp. By default, this is set to ``true``.";
    static final String ENRICHMENT_DOC = "This setting is used to enrich raw data with extra metadata fields. It " +
            "contains a list of key value pairs separated by ``,``. The configured enrichment metadata will be indexed " +
            "along with raw event data by Splunk. This is only applicable to the ``/event`` HEC endpoint " +
            "(``splunk.hec.raw=false``). Data enrichment for the ``/event`` HEC endpoint is only available in Splunk " +
            "Enterprise 6.5 and above. By default, this setting is empty.";
    static final String TRACK_DATA_DOC = "When set to ``true``, data loss and data injection latency metadata will be " +
            "indexed along with raw data. This setting only works in conjunction with ``/event`` HEC endpoint " +
            "(``splunk.hec.raw=false``).";
    static final String HEC_EVENT_FORMATTED_DOC = "This setting ensures events are pre-formatted into the `proper HEC " +
            "JSON format <http://dev.splunk.com/view/event-collector/SP-CAAAE6P>`_, have meta-data and event data so " +
            "that they are indexed correctly by Splunk.";
    // TBD
    static final String SSL_TRUSTSTORE_PATH_DOC = "Path on the local disk to the certificate trust store.";
    static final String SSL_TRUSTSTORE_PASSWORD_DOC = "Password for the trust store.";

    static final String HEADER_SUPPORT_DOC = "This setting enables Kafka Record headers to be used for meta data override.";
    static final String HEADER_CUSTOM_DOC = "This setting enables looking for Record headers with these values and " +
            "adding them to each event if present. Multiple headers are separated by comma. For example: " +
            "``custom_header_1,custom_header_2,custom_header_3``.";
    static final String HEADER_INDEX_DOC = "Header to use for Splunk Header Index.";
    static final String HEADER_SOURCE_DOC = "Header to use for Splunk Header Source.";
    static final String HEADER_SOURCETYPE_DOC = "Header to use for Splunk Header Sourcetype.";
    static final String HEADER_HOST_DOC = "Header to use for Splunk Header Host.";

    final String splunkToken;
    final String splunkURI;
    final Map<String, Map<String, String>> topicMetas;

    final String indexes;
    final String sourcetypes;
    final String sources;

    final int totalHecChannels;
    final int maxHttpConnPerChannel;
    final int maxBatchSize;
    final boolean httpKeepAlive;
    final int numberOfThreads;
    final int socketTimeout;
    final boolean validateCertificates;

    final boolean ack;
    final int ackPollInterval;
    final int ackPollThreads;
    final int eventBatchTimeout;
    final int maxOutstandingEvents;
    final int maxRetries;
    final int backoffThresholdSeconds;

    final boolean raw;
    final boolean hecEventFormatted;

    final String lineBreaker;
    final boolean useRecordTimestamp;
    final Map<String, String> enrichments;
    final boolean trackData;

    final boolean hasTrustStorePath;
    final String trustStorePath;
    final String trustStorePassword;

    final boolean headerSupport;
    final String headerCustom;
    final String headerIndex;
    final String headerSource;
    final String headerSourcetype;
    final String headerHost;

    SplunkSinkConnectorConfig(Map<String, String> taskConfig) {
        super(conf(), taskConfig);
        splunkToken = getPassword(TOKEN_CONF).value();
        splunkURI = getString(URI_CONF);
        raw = getBoolean(RAW_CONF);
        ack = getBoolean(ACK_CONF);
        indexes = getString(INDEX_CONF);
        sourcetypes = getString(SOURCETYPE_CONF);
        sources = getString(SOURCE_CONF);
        httpKeepAlive = getBoolean(HTTP_KEEPALIVE_CONF);
        validateCertificates = getBoolean(SSL_VALIDATE_CERTIFICATES_CONF);
        trustStorePath = getString(SSL_TRUSTSTORE_PATH_CONF);
        hasTrustStorePath = StringUtils.isNotBlank(trustStorePath);
        trustStorePassword = getPassword(SSL_TRUSTSTORE_PASSWORD_CONF).value();
        eventBatchTimeout = getInt(EVENT_TIMEOUT_CONF);
        ackPollInterval = getInt(ACK_POLL_INTERVAL_CONF);
        ackPollThreads = getInt(ACK_POLL_THREADS_CONF);
        maxHttpConnPerChannel = getInt(MAX_HTTP_CONNECTION_PER_CHANNEL_CONF);
        totalHecChannels = getInt(TOTAL_HEC_CHANNEL_CONF);
        socketTimeout = getInt(SOCKET_TIMEOUT_CONF);
        enrichments = parseEnrichments(getString(ENRICHMENT_CONF));
        trackData = getBoolean(TRACK_DATA_CONF);
        useRecordTimestamp = getBoolean(USE_RECORD_TIMESTAMP_CONF);
        maxBatchSize = getInt(MAX_BATCH_SIZE_CONF);
        numberOfThreads = getInt(HEC_THREDS_CONF);
        lineBreaker = getString(LINE_BREAKER_CONF);
        maxOutstandingEvents = getInt(MAX_OUTSTANDING_EVENTS_CONF);
        maxRetries = getInt(MAX_RETRIES_CONF);
        backoffThresholdSeconds = getInt(HEC_BACKOFF_PRESSURE_THRESHOLD);
        hecEventFormatted = getBoolean(HEC_EVENT_FORMATTED_CONF);
        topicMetas = initMetaMap(taskConfig);
        headerSupport = getBoolean(HEADER_SUPPORT_CONF);
        headerCustom = getString(HEADER_CUSTOM_CONF);
        headerIndex = getString(HEADER_INDEX_CONF);
        headerSource = getString(HEADER_SOURCE_CONF);
        headerSourcetype = getString(HEADER_SOURCETYPE_CONF);
        headerHost = getString(HEADER_HOST_CONF);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(TOKEN_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, TOKEN_DOC)
                .define(URI_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, URI_DOC)
                .define(RAW_CONF, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, RAW_DOC)
                .define(ACK_CONF, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, ACK_DOC)
                .define(INDEX_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, INDEX_DOC)
                .define(SOURCETYPE_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, SOURCETYPE_DOC)
                .define(SOURCE_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, SOURCE_DOC)
                .define(HTTP_KEEPALIVE_CONF, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM, HTTP_KEEPALIVE_DOC)
                .define(SSL_VALIDATE_CERTIFICATES_CONF, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM, SSL_VALIDATE_CERTIFICATES_DOC)
                .define(SSL_TRUSTSTORE_PATH_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, SSL_TRUSTSTORE_PATH_DOC)
                .define(SSL_TRUSTSTORE_PASSWORD_CONF, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.HIGH, SSL_TRUSTSTORE_PASSWORD_DOC)
                .define(EVENT_TIMEOUT_CONF, ConfigDef.Type.INT, 300, ConfigDef.Importance.MEDIUM, EVENT_TIMEOUT_DOC)
                .define(ACK_POLL_INTERVAL_CONF, ConfigDef.Type.INT, 10, ConfigDef.Importance.MEDIUM, ACK_POLL_INTERVAL_DOC)
                .define(ACK_POLL_THREADS_CONF, ConfigDef.Type.INT, 2, ConfigDef.Importance.MEDIUM, ACK_POLL_THREADS_DOC)
                .define(MAX_HTTP_CONNECTION_PER_CHANNEL_CONF, ConfigDef.Type.INT, 2, ConfigDef.Importance.MEDIUM, MAX_HTTP_CONNECTION_PER_CHANNEL_DOC)
                .define(TOTAL_HEC_CHANNEL_CONF, ConfigDef.Type.INT, 2, ConfigDef.Importance.HIGH, TOTAL_HEC_CHANNEL_DOC)
                .define(SOCKET_TIMEOUT_CONF, ConfigDef.Type.INT, 60, ConfigDef.Importance.LOW, SOCKET_TIMEOUT_DOC)
                .define(ENRICHMENT_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, ENRICHMENT_DOC)
                .define(TRACK_DATA_CONF, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, TRACK_DATA_DOC)
                .define(USE_RECORD_TIMESTAMP_CONF, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM, USE_RECORD_TIMESTAMP_DOC)
                .define(HEC_THREDS_CONF, ConfigDef.Type.INT, 1, ConfigDef.Importance.LOW, HEC_THREADS_DOC)
                .define(LINE_BREAKER_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, LINE_BREAKER_DOC)
                .define(MAX_OUTSTANDING_EVENTS_CONF, ConfigDef.Type.INT, 1000000, ConfigDef.Importance.MEDIUM, MAX_OUTSTANDING_EVENTS_DOC)
                .define(MAX_RETRIES_CONF, ConfigDef.Type.INT, -1, ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC)
                .define(HEC_BACKOFF_PRESSURE_THRESHOLD, ConfigDef.Type.INT, 60, ConfigDef.Importance.MEDIUM, HEC_BACKOFF_PRESSURE_THRESHOLD_DOC)
                .define(HEC_EVENT_FORMATTED_CONF, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, HEC_EVENT_FORMATTED_DOC)
                .define(MAX_BATCH_SIZE_CONF, ConfigDef.Type.INT, 500, ConfigDef.Importance.MEDIUM, MAX_BATCH_SIZE_DOC)
                .define(HEADER_SUPPORT_CONF, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, HEADER_SUPPORT_DOC)
                .define(HEADER_CUSTOM_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, HEADER_CUSTOM_DOC)
                .define(HEADER_INDEX_CONF, ConfigDef.Type.STRING, "splunk.header.index", ConfigDef.Importance.MEDIUM, HEADER_INDEX_DOC)
                .define(HEADER_SOURCE_CONF, ConfigDef.Type.STRING, "splunk.header.source", ConfigDef.Importance.MEDIUM, HEADER_SOURCE_DOC)
                .define(HEADER_SOURCETYPE_CONF, ConfigDef.Type.STRING, "splunk.header.sourcetype", ConfigDef.Importance.MEDIUM, HEADER_SOURCETYPE_DOC)
                .define(HEADER_HOST_CONF, ConfigDef.Type.STRING, "splunk.header.host", ConfigDef.Importance.MEDIUM, HEADER_HOST_DOC);
    }
    /**
     Configuration Method to setup all settings related to Splunk HEC Client
     */
    public HecConfig getHecConfig() {
        HecConfig config = new HecConfig(Arrays.asList(splunkURI.split(",")), splunkToken);
        config.setDisableSSLCertVerification(!validateCertificates)
              .setSocketTimeout(socketTimeout)
              .setMaxHttpConnectionPerChannel(maxHttpConnPerChannel)
              .setTotalChannels(totalHecChannels)
              .setEventBatchTimeout(eventBatchTimeout)
              .setHttpKeepAlive(httpKeepAlive)
              .setAckPollInterval(ackPollInterval)
              .setAckPollThreads(ackPollThreads)
              .setEnableChannelTracking(trackData)
              .setBackoffThresholdSeconds(backoffThresholdSeconds)
              .setTrustStorePath(trustStorePath)
              .setTrustStorePassword(trustStorePassword)
              .setHasCustomTrustStore(hasTrustStorePath);
        return config;
    }

    public boolean hasMetaDataConfigured() {
        return (indexes != null && !indexes.isEmpty()
                || (sources != null && !sources.isEmpty())
                || (sourcetypes != null && !sourcetypes.isEmpty()));
    }

    public String toString() {
        return "splunkURI:" + splunkURI + ", "
                + "raw:" + raw + ", "
                + "ack:" + ack + ", "
                + "indexes:" + indexes + ", "
                + "sourcetypes:" + sourcetypes + ", "
                + "sources:" + sources + ", "
                + "headerSupport:" + headerSupport + ", "
                + "headerCustom:" + headerCustom + ", "
                + "httpKeepAlive:" + httpKeepAlive + ", "
                + "validateCertificates:" + validateCertificates + ", "
                + "trustStorePath:" + trustStorePath + ", "
                + "socketTimeout:" + socketTimeout + ", "
                + "eventBatchTimeout:" + eventBatchTimeout + ", "
                + "ackPollInterval:" + ackPollInterval + ", "
                + "ackPollThreads:" + ackPollThreads + ", "
                + "maxHttpConnectionPerChannel:" + maxHttpConnPerChannel + ", "
                + "totalHecChannels:" + totalHecChannels + ", "
                + "enrichment:" + getString(ENRICHMENT_CONF) + ", "
                + "maxBatchSize:" + maxBatchSize + ", "
                + "numberOfThreads:" + numberOfThreads + ", "
                + "lineBreaker:" + lineBreaker + ", "
                + "maxOutstandingEvents:" + maxOutstandingEvents + ", "
                + "maxRetries:" + maxRetries + ", "
                + "useRecordTimestamp:" + useRecordTimestamp + ", "
                + "hecEventFormatted:" + hecEventFormatted + ", "
                + "trackData:" + trackData + ", "
                + "headerSupport:" + headerSupport + ", "
                + "headerCustom:" + headerCustom + ", "
                + "headerIndex:" + headerIndex + ", "
                + "headerSource:" + headerSource + ", "
                + "headerSourcetype:" + headerSourcetype + ", "
                + "headerHost:" + headerHost;
    }

    private static String[] split(String data, String sep) {
        if (data != null && !data.trim().isEmpty()) {
            return data.trim().split(sep);
        }
        return null;
    }

    private static Map<String, String> parseEnrichments(String enrichment) {
        String[] kvs = split(enrichment, ",");
        if (kvs == null) {
            return null;
        }

        Map<String, String> enrichmentKvs = new HashMap<>();
        for (final String kv: kvs) {
            String[] kvPairs = split(kv, "=");
            if (kvPairs.length != 2) {
                throw new ConfigException("Invalid enrichment: " + enrichment+ ". Expect key value pairs and separated by comma");
            }
            enrichmentKvs.put(kvPairs[0], kvPairs[1]);
        }
        return enrichmentKvs;
    }

    private String getMetaForTopic(String[] metas, int expectedLength, int curIdx, String confKey) {
        if (metas == null) {
            return null;
        }

        if (metas.length == 1) {
            return metas[0];
        } else if (metas.length == expectedLength) {
            return metas[curIdx];
        } else {
            throw new ConfigException("Invalid " + confKey + " configuration=" + metas);
        }
    }

    private Map<String, Map<String, String>> initMetaMap(Map<String, String> taskConfig) {
        String[] topics = split(taskConfig.get(SinkConnector.TOPICS_CONFIG), ",");
        String[] topicIndexes = split(indexes, ",");
        String[] topicSourcetypes = split(sourcetypes, ",");
        String[] topicSources = split(sources, ",");

        Map<String, Map<String, String>> metaMap = new HashMap<>();
        int idx = 0;
        for (String topic: topics) {
            HashMap<String, String> topicMeta = new HashMap<>();
            String meta = getMetaForTopic(topicIndexes, topics.length, idx, INDEX_CONF);
            if (meta != null) {
                topicMeta.put(INDEX, meta);
            }

            meta = getMetaForTopic(topicSourcetypes, topics.length, idx, SOURCETYPE_CONF);
            if (meta != null) {
                topicMeta.put(SOURCETYPE, meta);
            }

            meta = getMetaForTopic(topicSources, topics.length, idx, SOURCE_CONF);
            if (meta != null) {
                topicMeta.put(SOURCE, meta);
            }

            metaMap.put(topic, topicMeta);
            idx += 1;
        }
        return metaMap;
    }

    public static void main(String[] args) {
    System.out.println(conf().toEnrichedRst());
  }

}
