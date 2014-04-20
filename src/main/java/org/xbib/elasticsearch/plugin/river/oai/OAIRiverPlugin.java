
package org.xbib.elasticsearch.plugin.river.oai;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

public class OAIRiverPlugin extends AbstractPlugin {

    @Inject
    public OAIRiverPlugin() {
    }

    @Override
    public String name() {
        return "oai-river" + "-"
                + Build.getInstance().getVersion() + "-"
                + Build.getInstance().getShortHash();
    }

    @Override
    public String description() {
        return "OAI River Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("oai", OAIRiverModule.class);
    }
}
