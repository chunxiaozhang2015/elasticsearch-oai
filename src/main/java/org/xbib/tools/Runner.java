package org.xbib.tools;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class Runner {

    public static void main(String[] args) {
        try {
            Class clazz = Class.forName(args[0]);
            CommandLineInterpreter commandLineInterpreter = (CommandLineInterpreter) clazz.newInstance();
            commandLineInterpreter.reader(new InputStreamReader(System.in, "UTF-8"))
                    .writer(new OutputStreamWriter(System.out, "UTF-8"))
                    .run();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}
