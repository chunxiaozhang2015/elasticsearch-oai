package org.xbib.elasticsearch.plugin.oai;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.Plugin;

public class OAIPlugin extends Plugin {

    @Inject
    public OAIPlugin() {
    }

    @Override
    public String name() {
        return "oai";
    }

    @Override
    public String description() {
        return "OAI Plugin";
    }


}
