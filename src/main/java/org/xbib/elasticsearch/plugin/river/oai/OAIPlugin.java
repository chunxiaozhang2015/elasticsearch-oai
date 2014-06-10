package org.xbib.elasticsearch.plugin.river.oai;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

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

    public void onModule(RiversModule module) {
        module.registerRiver("oai", OAIRiverModule.class);
    }

}
