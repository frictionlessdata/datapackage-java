package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.frictionlessdata.datapackage.Package;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.tableschema.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Abstract base class for all Resources that are based on directly set tabular data, that is not on
 * data specified as files or URLs.
 *
 * @param <T> the data format, either CSV or JSON array
 */
@JsonInclude(value= JsonInclude.Include. NON_EMPTY, content= JsonInclude.Include. NON_NULL)
public abstract class AbstractDataResource<T> extends AbstractResource<T> {
    @JsonIgnore
    T data;

    AbstractDataResource(String name, T data) {
        super(name);
        this.data = data;
        super.format = Resource.FORMAT_JSON;
        serializeToFile = false;
        if (data == null)
            throw new DataPackageException("Invalid Resource. The data property cannot be null for a Data-based Resource.");
    }


    @Override
    @JsonProperty(JSON_KEY_DATA)
    public Object getRawData() throws IOException {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setDataPoperty(T data) {
        this.data = data;
    }

    @Override
    @JsonIgnore
    List<Table> readData () throws Exception{
        List<Table> tables = new ArrayList<>();
        if (data != null){
            if (format.equalsIgnoreCase(getResourceFormat())){
                Table table = Table.fromSource(data.toString(), schema, getCsvFormat());
                tables.add(table);
            } else {
                // Data is in unexpected format. Throw exception.
                throw new DataPackageException("A resource has an invalid data format. It should be a CSV String or a JSON Array.");
            }
        } else{
            throw new DataPackageException("No data has been set.");
        }
        return tables;
    }

    @Override
    @JsonIgnore
    public Set<String> getDatafileNamesForWriting() {
        String name = super.getName()
                .toLowerCase()
                .replaceAll("\\W", "_");
        Set<String> names = new HashSet<>();
        names.add(JSON_KEY_DATA+"/"+name);
        return names;
    }

    @Override
    public void validate(Package pkg) throws DataPackageValidationException {
        super.validate(pkg);
        try {
            if (getRawData() == null) {
                throw new DataPackageValidationException("Data resource must have data");
            }

            if (getFormat() == null) {
                throw new DataPackageValidationException("Data resource must specify a format");
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


    @JsonIgnore
    abstract String getResourceFormat();
}
