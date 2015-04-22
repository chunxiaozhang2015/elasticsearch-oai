package org.xbib.tools;

import java.io.Reader;
import java.io.Writer;

public interface CommandLineInterpreter {

    CommandLineInterpreter reader(Reader reader);

    CommandLineInterpreter writer(Writer writer);

    void run() throws Exception;

}
