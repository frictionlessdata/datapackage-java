package io.frictionlessdata.datapackage.datareference;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;

public class UrlDataReference extends AbstractDataReference<URL>{


    UrlDataReference(Collection<URL> source) {
        super(source);
    }

    @Override
    public Collection<String> getReferencesAsStrings() {
        Collection<URL> elements = resolve();
        return elements
                .stream()
                .map((u) -> u.toExternalForm())
                .collect(Collectors.toList());
    }
}
