package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.frictionlessdata.tableschema.Table;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@JsonInclude(value= JsonInclude.Include. NON_EMPTY, content= JsonInclude.Include. NON_NULL)
public class URLbasedResource extends AbstractReferencebasedResource<URL> {

    public URLbasedResource(String name, Collection<URL> paths) {
        super(name, paths);
        serializeToFile = false;
    }

    @Override
    Table createTable(URL reference) throws Exception {
        return Table.fromSource(reference, schema, getCsvFormat());
    }

    @Override
    byte[] getRawData(URL input)  throws IOException {
        try (InputStream inputStream = input.openStream()) {
            return getRawData(inputStream);
        }
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
}
