package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.Table;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for all Resources that are based on directly set data, that is not on
 * data specified as files or URLs.
 *
 * @param <T> the data format, either CSV or JSON array
 */
public abstract class AbstractDataResource<T,C> extends AbstractResource<T,C> {
    private T data;

    public AbstractDataResource(String name, T data) {
        super(name);
        this.data = data;
        super.format = Resource.FORMAT_JSON;
        if (data == null)
            throw new DataPackageException("Invalid Resource. The data property cannot be null for a Data-based Resource.");
    }

    /**
     * @return the data
     */
    public T getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(T data) {
        this.data = data;
    }

    @Override
    List<Table> readData () throws Exception{
        List<Table> tables = new ArrayList<>();
        if (data != null){
            if (format.equalsIgnoreCase(getResourceFormat())){
                Table table = new Table(data.toString(), schema, getCsvFormat());
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
    public void writeDataAsCsv(Path outputDir, Dialect dialect) throws Exception {
        Dialect lDialect = (null != dialect) ? dialect : Dialect.DEFAULT;
        String fileName = super.getName()
                .toLowerCase()
                .replaceAll("\\W", "_");
        List<Table> tables = getTables();
        Path p = outputDir.resolve(fileName);
        writeTableAsCsv(tables.get(0), lDialect, p);
    }

    abstract String getResourceFormat();
}
