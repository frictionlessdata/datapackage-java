package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.util.JsonUtil;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class AbstractReferencebasedResource<T,C> extends AbstractResource<T,C> {
    Collection<T> paths;

    AbstractReferencebasedResource(String name, Collection<T> paths) {
        super(name);
        this.paths = paths;
    }

    public Collection<String> getReferencesAsStrings() {
        final Collection<String> strings = new ArrayList<>();
        if (null != paths) {
            paths.forEach((p) -> strings.add(getStringRepresentation(p)));
        }
        return strings;
    }

    /*
     if more than one path in our paths object, return a JSON array,
     else just that one object.
     */
    JsonNode getPathJson() {
        List<String> path = new ArrayList<>(getReferencesAsStrings());
        if (path.size() == 1) {
            return JsonUtil.getInstance().createTextNode(path.get(0));
        } else {
            return JsonUtil.getInstance().createArrayNode(path);
        }
    }

    public Collection<T> getPaths() {
        return paths;
    }

    abstract Table createTable(T reference) throws Exception;

    abstract String getStringRepresentation(T reference);

}
