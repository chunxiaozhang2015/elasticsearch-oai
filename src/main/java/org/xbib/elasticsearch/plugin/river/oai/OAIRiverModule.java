
package org.xbib.elasticsearch.plugin.river.oai;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class OAIRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(OAIRiver.class).asEagerSingleton();
    }
}
