package io.frictionlessdata.datapackage.datareference;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.datasources.CsvDataSource;
import io.frictionlessdata.tableschema.datasources.JsonArrayDataSource;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public abstract class AbstractDataReference<T> implements DataReference {
    Set<T> elements;

    AbstractDataReference(Collection<T> source) {
        elements = new LinkedHashSet<>(source);
    }

    @Override
    public Collection<T> resolve() {
        return elements;
    }


    @Override
    public JSONArray getJson() {
        if (this.elements == null) {
            return null;
        }
        return new JSONArray(getReferencesAsStrings());
    }
}
