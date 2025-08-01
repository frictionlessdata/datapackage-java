package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.frictionlessdata.datapackage.Package;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import io.frictionlessdata.tableschema.util.JsonUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

@JsonInclude(value= JsonInclude.Include. NON_EMPTY, content= JsonInclude.Include. NON_NULL)
public abstract class AbstractReferencebasedResource<T> extends AbstractResource<T> {
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

    @Override
    @JsonIgnore
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
    @JsonProperty(JSON_KEY_PATH)
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
        List<String> paths = new ArrayList<>(this.getReferencesAsStrings());
        return paths.stream().map((p) -> {
            if (p.toLowerCase().endsWith("."+ TableDataSource.Format.FORMAT_CSV.getLabel())){
                int i = p.toLowerCase().indexOf("."+TableDataSource.Format.FORMAT_CSV.getLabel());
                return p.substring(0, i);
            } else if (p.toLowerCase().endsWith("."+TableDataSource.Format.FORMAT_JSON.getLabel())){
                int i = p.toLowerCase().indexOf("."+TableDataSource.Format.FORMAT_JSON.getLabel());
                return p.substring(0, i);
            } else {
                int i = p.lastIndexOf(".");
                return p.substring(0, i);
            }
        }).collect(Collectors.toSet());
    }

    @Override
    public void validate(Package pkg) throws DataPackageValidationException {
        super.validate(pkg);
        List<T> paths = new ArrayList<>(getPaths());
        try {
            if (paths.isEmpty()) {
                throw new DataPackageValidationException("File- or URL-based resource must have at least one path");
            }

            for (T path : paths) {
                String name = null;
                if (path instanceof File) {
                    name = ((File) path).getName();
                } else if (path instanceof URL) {
                    name = ((URL) path).getPath();
                }
                if (name == null || name.trim().isEmpty()) {
                    throw new DataPackageValidationException("Resource path cannot be null or empty");
                }
            }
        } catch (Exception ex) {
            if (ex instanceof DataPackageValidationException) {
                errors.add((DataPackageValidationException) ex);
            }
            else {
                errors.add(new DataPackageValidationException(ex));
            }
        }
    }


    static String sniffFormat(Collection<?> paths) {
        Set<String> foundFormats = new HashSet<>();
        for (Object p : paths) {
            String name;
            if (p instanceof String) {
                name = (String) p;
            } else if (p instanceof File) {
                name = ((File) p).getName();
            } else if (p instanceof URL) {
                name = ((URL) p).getPath();
            } else {
                throw new DataPackageException("Unsupported path type: " + p.getClass().getName());
            }
            if (name.toLowerCase().endsWith(TableDataSource.Format.FORMAT_CSV.getLabel())) {
                foundFormats.add(TableDataSource.Format.FORMAT_CSV.getLabel());
            } else if (name.toLowerCase().endsWith(TableDataSource.Format.FORMAT_JSON.getLabel())) {
                foundFormats.add(TableDataSource.Format.FORMAT_JSON.getLabel());
            } else {
                // something else -> not a tabular resource
                int pos = name.lastIndexOf('.');
                return name.substring(pos + 1).toLowerCase();
            }
        }
        if (foundFormats.size() > 1) {
            throw new DataPackageException("Resources cannot be mixed JSON/CSV");
        }
        if (foundFormats.isEmpty())
            return TableDataSource.Format.FORMAT_CSV.getLabel();
        return foundFormats.iterator().next();
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
