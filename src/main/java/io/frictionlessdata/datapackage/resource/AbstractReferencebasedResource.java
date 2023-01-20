package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import io.frictionlessdata.tableschema.util.JsonUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
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

    @JsonIgnore
    @Override
    public Object getRawData() throws IOException {
        // If the path(s) of data file/URLs has been set.
        if (paths != null){
            Iterator<T> iter = paths.iterator();
            if (paths.size() == 1) {
                return getRawData(iter.next());
            } else {
                // this is probably not very useful, but it's the spec...
                byte[][] retVal = new byte[paths.size()][];
                for (int i = 0; i < paths.size(); i++) {
                    byte[] bytes = getRawData(iter.next());
                    retVal[i] = bytes;
                }
                return retVal;
            }
        }
        return null;
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

    abstract Table createTable(T reference) throws Exception;

    abstract String getStringRepresentation(T reference);

    abstract byte[] getRawData(T input) throws IOException;
    byte[] getRawData(InputStream inputStream) throws IOException {
        byte[] retVal = null;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            for (int b; (b = inputStream.read()) != -1; ) {
                out.write(b);
            }
            retVal = out.toByteArray();
        }
        return retVal;
    }

}
