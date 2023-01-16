package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.tableschema.Table;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class URLbasedResource<C> extends AbstractReferencebasedResource<URL, C> {

    public URLbasedResource(String name, Collection<URL> paths) {
        super(name, paths);
        serializeToFile = false;
    }

    @Override
    Table createTable(URL reference, Charset encoding) throws Exception {
        return Table.fromSource(reference, schema, getCsvFormat(), encoding);
    }

    @Override
    String getStringRepresentation(URL reference) {
        return reference.toExternalForm();
    }

    @Override
    List<Table> readData () throws Exception{
        List<Table> tables = new ArrayList<>();
        Charset cs = getEncodingOrDefault();
        // If the path of a data file has been set.
        if (super.paths != null){
            for (URL url : paths) {
                Table table = createTable(url, cs);
                tables.add(table);
            }
        }
        return tables;
    }
}
