package io.frictionlessdata.datapackage.resource;

public class CSVDataResource extends AbstractDataResource<String> {

    public CSVDataResource(String name, String data) {
        super(name, data);
        super.format = getResourceFormat();
    }

    @Override
    String getResourceFormat() {
        return Resource.FORMAT_CSV;
    }
}
