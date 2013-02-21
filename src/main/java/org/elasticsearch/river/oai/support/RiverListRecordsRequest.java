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
package org.elasticsearch.river.oai.support;

import org.xbib.oai.ListRecordsRequest;
import org.xbib.oai.ResumptionToken;

import java.net.URI;
import java.util.Date;
import java.util.Map;

public class RiverListRecordsRequest extends ListRecordsRequest {

    Date from;
    Date until;
    ResumptionToken token;
    String oaiSet;
    String oaiMetadataPrefix;

    RiverListRecordsRequest(URI uri, String oaiSet, String oaiMetadataPrefix) {
        super(uri);
        this.oaiSet = oaiSet;
        this.oaiMetadataPrefix = oaiMetadataPrefix;
    }

    @Override
    public void setFrom(Date from) {
        this.from = from;
    }

    @Override
    public Date getFrom() {
        return from;
    }

    @Override
    public void setUntil(Date until) {
        this.until = until;
    }

    @Override
    public Date getUntil() {
        return until;
    }

    @Override
    public String getSet() {
        return oaiSet;
    }

    @Override
    public String getMetadataPrefix() {
        return oaiMetadataPrefix;
    }

    @Override
    public void setResumptionToken(ResumptionToken token) {
        this.token = token;
    }

    @Override
    public ResumptionToken getResumptionToken() {
        return token;
    }

    @Override
    public String getPath() {
        // only used in server context
        return null;
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        // only used in server context
        return null;
    }
}