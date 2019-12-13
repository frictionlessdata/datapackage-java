package io.frictionlessdata.datapackage.resource;

import org.json.JSONArray;

public class JSONDataResource<C> extends AbstractDataResource<JSONArray,C> {

    public JSONDataResource(String name, JSONArray data) {
        super(name, data);
        super.format = getResourceFormat();
    }

    @Override
    String getResourceFormat() {
        return Resource.FORMAT_JSON;
    }
}
