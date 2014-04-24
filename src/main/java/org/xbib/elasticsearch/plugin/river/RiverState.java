
package org.xbib.elasticsearch.plugin.river;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;

import org.xbib.common.settings.ImmutableSettings;
import org.xbib.common.settings.Settings;
import org.xbib.common.xcontent.ToXContent;
import org.xbib.common.xcontent.XContentBuilder;
import org.xbib.common.xcontent.XContentFactory;
import org.xbib.common.xcontent.XContentParser;
import org.xbib.common.xcontent.XContentType;
import org.xbib.logging.Logger;
import org.xbib.logging.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.xbib.common.xcontent.XContentFactory.jsonBuilder;
import static org.xbib.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.xbib.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.xbib.common.xcontent.XContentParser.Token.START_OBJECT;
import static org.xbib.common.xcontent.XContentParser.Token.VALUE_BOOLEAN;
import static org.xbib.common.xcontent.XContentParser.Token.VALUE_NULL;
import static org.xbib.common.xcontent.XContentParser.Token.VALUE_NUMBER;
import static org.xbib.common.xcontent.XContentParser.Token.VALUE_STRING;

public class RiverState implements ToXContent {

    private final static Logger logger = LoggerFactory.getLogger(RiverState.class.getSimpleName());

    /**
     * The date the river was started
     */
    private Date started;

    /**
     * A name of an object the river is currently processing
     */
    private String name;

    /**
     * The type of the river
     */
    private String type;

    /**
     * Last timestamp of river activity
     */
    private Date timestamp;

    /**
     * A counter for river activity
     */
    private long counter;

    /**
     * A flag for signalling river activity
     */
    private boolean active;

    /**
     * A custom map for more information about the river
     */
    private Map<String,Object> custom;

    /**
     * The settings of the river
     */
    private Settings settings;

    /**
     * Coordinate for the state persistence document
     */
    private String coordinateIndex;

    /**
     * Coordinate for the state persistence document
     */
    private String coordinateType;

    /**
     * Coordinate for the state persistence document
     */
    private String coordinateId;

    public RiverState(Date started) {
        this.started = started;
    }

    public Date getStarted() {
        return started;
    }

    public RiverState setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public RiverState setType(String type) {
        this.type = type;
        return this;
    }

    public String getType() {
        return type;
    }

    public RiverState setCounter(long counter) {
        this.counter = counter;
        return this;
    }

    public long getCounter() {
        return counter;
    }

    public RiverState setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public RiverState setActive(boolean active) {
        this.active = active;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public RiverState setCustom(Map<String,Object> custom) {
        this.custom = custom;
        return this;
    }

    public Map<String,Object> getCustom() {
        return custom;
    }

    public RiverState setSettings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public Settings getSettings() {
        return settings;
    }

    public RiverState setCoordinates(String index, String type, String id) {
        this.coordinateIndex = index;
        this.coordinateType = type;
        this.coordinateId = id;
        return this;
    }

    public RiverState save(Client client) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder = toXContent(builder, ToXContent.EMPTY_PARAMS);
        if (logger.isDebugEnabled()) {
            logger.debug("save state={}", builder.string());
        }
        client.prepareIndex()
                .setIndex(coordinateIndex)
                .setType(coordinateType)
                .setId(coordinateId)
                .setRefresh(true)
                .setSource(builder.string())
                .execute()
                .actionGet();
        return this;
    }

    public RiverState load(Client client) throws IOException {
        GetResponse get = null;
        try {
            client.admin().indices().prepareRefresh(coordinateIndex).execute().actionGet();
            get = client.prepareGet(coordinateIndex, coordinateType, coordinateId).execute().actionGet();
        } catch (Exception e) {
            logger.warn("state not found: {}/{}/{}", coordinateIndex, coordinateType, coordinateId);
        }
        if (get != null && get.isExists()) {
            if (logger.isDebugEnabled()) {
                logger.debug("load state={}", get.getSourceAsString());
            }
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(get.getSourceAsBytes());
            fromXContent(parser);
        } else {
            counter = 0L;
        }
        return this;
    }

    public void fromXContent(XContentParser parser) throws IOException {
        DateMathParser dateParser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != END_OBJECT) {
            if (token == null) {
                break;
            } else if (token == FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == VALUE_NULL || token == VALUE_STRING
                    || token == VALUE_BOOLEAN || token == VALUE_NUMBER) {
                switch (currentFieldName) {
                    case "name":
                        setName(parser.text());
                        break;
                    case "type":
                        setType(parser.text());
                        break;
                    case "started":
                        try {
                            this.started = new Date(dateParser.parse(parser.text(), 0));
                        } catch (Exception e) {
                            // ignore
                        }
                        break;
                    case "timestamp":
                        try {
                            setTimestamp(new Date(dateParser.parse(parser.text(), 0)));
                        } catch (Exception e) {
                            // ignore
                        }
                        break;
                    case "counter":
                        try {
                            setCounter(parser.longValue());
                        } catch (Exception e) {
                            // ignore
                        }
                        break;
                    case "active":
                        setActive(parser.booleanValue());
                        break;
                }
            } else if (token == START_OBJECT) {
                if ("custom".equals(currentFieldName)) {
                    setCustom(parser.map());
                } else if ("settings".equals(currentFieldName)) {
                    setSettings(ImmutableSettings.readSettingsFromMap(parser.map()));
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
                .fieldIfNotNull("name", name)
                .fieldIfNotNull("type", type)
                .fieldIfNotNull("started", started)
                .fieldIfNotNull("timestamp", timestamp)
                .fieldIfNotNull("counter", counter)
                .fieldIfNotNull("active", active)
                .fieldIfNotNull("custom", custom)
                .fieldIfNotNull("settings", settings != null ? settings.getAsMap() : null)
            .endObject();
        return builder;
    }
}
