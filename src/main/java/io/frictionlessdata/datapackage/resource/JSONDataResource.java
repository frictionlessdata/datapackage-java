package io.frictionlessdata.datapackage.resource;

import org.json.JSONArray;

public class JSONDataResource extends AbstractDataResource<JSONArray> {

    public JSONDataResource(String name, JSONArray data) {
        super(name, data);
        super.format = getResourceFormat();
    }

    @Override
    String getResourceFormat() {
        return Resource.FORMAT_JSON;
    }
}
