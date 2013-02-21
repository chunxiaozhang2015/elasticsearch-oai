/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.river.oai;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.oai.support.Harvester;

import java.util.Map;

/**
 *
 */
public class OAIRiver extends AbstractRiverComponent implements River {

    private volatile Harvester harvester;
    private volatile Thread harvesterThread;

    @Inject
    public OAIRiver(RiverName riverName, RiverSettings settings,
                    @RiverIndexName String riverIndexName, Client client) {
        super(riverName, settings);

        harvester = new Harvester()
                .client(client)
                .riverIndexName(riverIndexName)
                .riverName(riverName.name());

        if (settings.settings().containsKey("oai")) {
            Map<String, Object> oaiSettings = (Map<String, Object>) settings.settings().get("oai");
            harvester
                    .oaiUrl(XContentMapValues.nodeStringValue(oaiSettings.get("url"), "http://localhost"))
                    .oaiPoll(XContentMapValues.nodeTimeValue(oaiSettings.get("poll"), TimeValue.timeValueMinutes(60)))
                    .oaiMetadataPrefix(XContentMapValues.nodeStringValue(oaiSettings.get("metadataPrefix"), "oai_dc"))
                    .oaiSet(XContentMapValues.nodeStringValue(oaiSettings.get("set"), null))
                    .oaiFrom(XContentMapValues.nodeStringValue(oaiSettings.get("from"), null))
                    .oaiUntil(XContentMapValues.nodeStringValue(oaiSettings.get("until"), null))
                    .oaiProxyHost(XContentMapValues.nodeStringValue(oaiSettings.get("proxyhost"), null))
                    .oaiProxyPort(XContentMapValues.nodeIntegerValue(oaiSettings.get("proxyport"), 0))
                    .oaiTimeout(XContentMapValues.nodeTimeValue(oaiSettings.get("timeout"), TimeValue.timeValueSeconds(60)));
        }
        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            harvester
                    .index(XContentMapValues.nodeStringValue(indexSettings.get("index"), "oai"))
                    .type(XContentMapValues.nodeStringValue(indexSettings.get("type"), "oai"))
                    .maxBulkActions(XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100))
                    .maxConcurrentRequests(XContentMapValues.nodeIntegerValue(indexSettings.get("max_bulk_requests"), 30));
        }
    }

    @Override
    public void start() {
        harvesterThread = EsExecutors.daemonThreadFactory(settings.globalSettings(),
                "oai_river(" + riverName().name() + ")").newThread(harvester);
        harvesterThread.start();
    }

    @Override
    public void close() {
        logger.info("closing OAI river {}", riverName.name());
        harvester.setClose(true);
        harvesterThread.interrupt();
    }

}
