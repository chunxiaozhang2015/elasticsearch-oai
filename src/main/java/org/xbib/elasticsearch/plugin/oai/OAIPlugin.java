package org.xbib.elasticsearch.plugin.oai;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;

public class OAIPlugin extends AbstractPlugin {

    @Inject
    public OAIPlugin() {
    }

    @Override
    public String name() {
        return "oai-"
                + Build.getInstance().getVersion() + "-"
                + Build.getInstance().getShortHash();
    }

    @Override
    public String description() {
        return "OAI Plugin";
    }


}
