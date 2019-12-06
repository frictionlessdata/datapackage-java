package io.frictionlessdata.datapackage.datareference;

import org.json.JSONArray;

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
