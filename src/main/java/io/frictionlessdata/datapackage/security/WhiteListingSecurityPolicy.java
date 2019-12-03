package io.frictionlessdata.datapackage.security;

import java.io.File;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class WhiteListingSecurityPolicy extends SecurityPolicy {
    private Set<String> urls = new HashSet<>();

    public WhiteListingSecurityPolicy() {
        SecurityInterceptor urlInterceptor = new SecurityInterceptor<URL>() {
            @Override
            public boolean accept(URL reference) {
                String refUrl = reference.toExternalForm();
                for (String testUrl : urls) {
                    if (refUrl.startsWith(testUrl))
                    return true;
                }
                return false;
            }
        };
        super.addInterceptor(urlInterceptor, URL.class);

        SecurityInterceptor fileInterceptor = new SecurityInterceptor<File>() {
            @Override
            public boolean accept(File reference) {
                return false;
            }
        };
        super.addInterceptor(fileInterceptor, File.class);
    }
}
