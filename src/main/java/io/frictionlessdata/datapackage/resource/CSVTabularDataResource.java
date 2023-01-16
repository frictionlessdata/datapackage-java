package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;

public class CSVTabularDataResource<C> extends AbstractDataResource<String,C> {

    public CSVTabularDataResource(String name, String data) {
        super(name, data);
        super.format = getResourceFormat();
        super.profile = Profile.PROFILE_TABULAR_DATA_RESOURCE;
    }

    @Override
    String getResourceFormat() {
        return TableDataSource.Format.FORMAT_CSV.getLabel();
    }
}
