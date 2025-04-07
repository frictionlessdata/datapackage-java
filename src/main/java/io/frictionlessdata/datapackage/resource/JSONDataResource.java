package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import io.frictionlessdata.tableschema.util.JsonUtil;

@JsonInclude(value= JsonInclude.Include. NON_EMPTY, content= JsonInclude.Include. NON_NULL)
public class JSONDataResource extends AbstractDataResource<ArrayNode> {

    public JSONDataResource(String name, String json) {
        super(name, JsonUtil.getInstance().createArrayNode(json));
        super.format = getResourceFormat();
    }

    @Override
    String getResourceFormat() {
        return TableDataSource.Format.FORMAT_JSON.getLabel();
    }

    @Override
    public String getProfile() {
        return Profile.PROFILE_TABULAR_DATA_RESOURCE;
    }
}
