
package org.xbib.elasticsearch.plugin.river;

import org.elasticsearch.river.River;

public interface StatefulRiver extends River {

    RiverState getRiverState();
}
