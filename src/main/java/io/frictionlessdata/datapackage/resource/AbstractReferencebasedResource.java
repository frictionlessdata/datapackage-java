package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import io.frictionlessdata.tableschema.util.JsonUtil;

import java.nio.charset.Charset;
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

    @JsonIgnore
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
    @JsonIgnore
    JsonNode getPathJson() {
        List<String> path = new ArrayList<>(getReferencesAsStrings());
        if (path.size() == 1) {
            return JsonUtil.getInstance().createTextNode(path.get(0));
        } else {
            return JsonUtil.getInstance().createArrayNode(path);
        }
    }

    @JsonIgnore
    public Collection<T> getPaths() {
        return paths;
    }

    @Override
    @JsonIgnore
    public Set<String> getDatafileNamesForWriting() {
        List<String> paths = new ArrayList<>(((FilebasedResource)this).getReferencesAsStrings());
        return paths.stream().map((p) -> {
            if (p.toLowerCase().endsWith("."+ TableDataSource.Format.FORMAT_CSV.getLabel())){
                int i = p.toLowerCase().indexOf("."+TableDataSource.Format.FORMAT_CSV.getLabel());
                return p.substring(0, i);
            } else if (p.toLowerCase().endsWith("."+TableDataSource.Format.FORMAT_JSON.getLabel())){
                int i = p.toLowerCase().indexOf("."+TableDataSource.Format.FORMAT_JSON.getLabel());
                return p.substring(0, i);
            }
            return p;
        }).collect(Collectors.toSet());
    }

    abstract Table createTable(T reference, Charset encoding) throws Exception;

    abstract String getStringRepresentation(T reference);

}
