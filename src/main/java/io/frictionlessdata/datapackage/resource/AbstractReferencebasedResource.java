package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.tableschema.Table;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    Object getPathJson() {
        List<String> path = new ArrayList<>(getReferencesAsStrings());
        if (path.size() == 1) {
            return path.get(0);
        } else {
            JSONArray pathNames = new JSONArray();
            for (Object obj : path) {
                pathNames.put(obj);

            }
            return pathNames;
        }
    }

    public Collection<T> getPaths() {
        return paths;
    }

    @Override
    Set<String> getDatafileNamesForWriting() {
        List<String> paths = new ArrayList<>(((FilebasedResource)this).getReferencesAsStrings());
        return paths.stream().map((p) -> {
            if (p.toLowerCase().endsWith("."+Resource.FORMAT_CSV)){
                int i = p.toLowerCase().indexOf("."+Resource.FORMAT_CSV);
                return p.substring(0, i);
            } else if (p.toLowerCase().endsWith("."+Resource.FORMAT_JSON)){
                int i = p.toLowerCase().indexOf("."+Resource.FORMAT_JSON);
                return p.substring(0, i);
            }
            return p;
        }).collect(Collectors.toSet());
    }

    abstract Table createTable(T reference) throws Exception;

    abstract String getStringRepresentation(T reference);

}
