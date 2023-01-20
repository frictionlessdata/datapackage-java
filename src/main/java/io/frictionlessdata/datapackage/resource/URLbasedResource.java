package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.tableschema.Table;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
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
