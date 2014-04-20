
package org.xbib.elasticsearch.plugin.feeder;

import java.io.InputStreamReader;

public class Runner {

    public static void main(String[] args) {
        try {
            Class clazz = Class.forName(args[0]);
            Tool tool = (Tool)clazz.newInstance();
            tool.readFrom(new InputStreamReader(System.in, "UTF-8")).run();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
