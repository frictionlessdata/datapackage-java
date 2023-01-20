package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.io.ByteStreams;
import io.frictionlessdata.tableschema.Table;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class URLbasedResource<C> extends AbstractReferencebasedResource<URL, C> {

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
