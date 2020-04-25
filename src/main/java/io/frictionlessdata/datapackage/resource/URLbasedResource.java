package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.Table;
import org.apache.commons.csv.CSVFormat;

import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.frictionlessdata.datapackage.Package.isValidUrl;

public class URLbasedResource<C> extends AbstractReferencebasedResource<URL, C> {

    public URLbasedResource(String name, Collection<URL> paths) {
        super(name, paths);
    }

    @Override
    Table createTable(URL reference) throws Exception {
        return Table.fromSource(reference, schema, getCsvFormat());
    }

    @Override
    String getStringRepresentation(URL reference) {
        return reference.toExternalForm();
    }

    @Override
    List<Table> readData () throws Exception{
        List<Table> tables = new ArrayList<>();
        // If the path of a data file has been set.
        if (super.paths != null){
            for (URL url : paths) {
                Table table = createTable(url);
                tables.add(table);
            }
        }
        return tables;
    }


    @Override
    public void writeDataAsCsv(Path outputDir, Dialect dialect) throws Exception {
        Dialect lDialect = (null != dialect) ? dialect : Dialect.DEFAULT;
        List<String> paths = new ArrayList<>(getReferencesAsStrings());
        int cnt = 0;
        for (String path : paths) {
            String fileName;
            if (isValidUrl(path)) {
                URL url = new URL (path);
                fileName = url.getFile();
            } else {
                throw new DataPackageException("Cannot writeDataAsCsv for "+path);
            }
            List<Table> tables = getTables();
            Table t  = tables.get(cnt++);
            Path p = outputDir.resolve(fileName);
            writeTableAsCsv(t, lDialect, p);
        }
    }
}
