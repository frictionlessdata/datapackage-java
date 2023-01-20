package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Abstract base class for all Resources that are based on directly set data, that is not on
 * data specified as files or URLs.
 *
 * @param <T> the data format, either CSV or JSON array
 */
public abstract class AbstractDataResource<T,C> extends AbstractResource<T,C> {
    T data;

    AbstractDataResource(String name, T data) {
        super(name);
        this.data = data;
        super.format = Resource.FORMAT_JSON;
        serializeToFile = false;
        if (data == null)
            throw new DataPackageException("Invalid Resource. The data property cannot be null for a Data-based Resource.");
    }

    /**
     * @return the data
     */
    @JsonIgnore
    @Override
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

    /**
     * write out any resource to a CSV file. It creates a file with a file name taken from
     * the Resource name. Subclasses might override this to write data differently (eg. to the
     * same files it was read from.
     * @param outputDir the directory to write to.
     * @param dialect the CSV dialect to use for writing
     * @throws Exception thrown if writing fails.
     */

    public void writeDataAsCsv(Path outputDir, Dialect dialect) throws Exception {
        Dialect lDialect = (null != dialect) ? dialect : Dialect.DEFAULT;
        String fileName = super.getName()
                .toLowerCase()
                .replaceAll("\\W", "_")
                +".csv";
        List<Table> tables = getTables();
        Path p = outputDir.resolve(fileName);
        writeTableAsCsv(tables.get(0), lDialect, p);
    }

    abstract String getResourceFormat();
}
