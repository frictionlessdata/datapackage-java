package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.frictionlessdata.datapackage.Profile;

@JsonInclude(value= JsonInclude.Include. NON_EMPTY, content= JsonInclude.Include. NON_NULL)
public class CSVDataResource extends AbstractDataResource<String> {

    public CSVDataResource(String name, String data) {
        super(name, data);
        super.format = getResourceFormat();
    }

    @Override
    String getResourceFormat() {
        return Resource.FORMAT_CSV;
    }

    @Override
    public String getProfile() {
        return Profile.PROFILE_TABULAR_DATA_RESOURCE;
    }
}
