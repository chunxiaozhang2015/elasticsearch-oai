package org.xbib.tools;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class ClasspathURLStreamHandler extends URLStreamHandler {

    private final ClassLoader classLoader;

    public ClasspathURLStreamHandler() {
        this.classLoader = getClass().getClassLoader();
    }

    @Override
    protected URLConnection openConnection(URL u) throws IOException {
        final URL resourceUrl = classLoader.getResource(u.getPath());
        return resourceUrl != null ? resourceUrl.openConnection() : null;
    }

}
