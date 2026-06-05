package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.node.ArrayNode;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import io.frictionlessdata.tableschema.util.JsonUtil;

@JsonInclude(value= JsonInclude.Include. NON_EMPTY, content= JsonInclude.Include. NON_NULL)
public class JSONDataResource extends AbstractDataResource<ArrayNode> {

    public JSONDataResource(String name, String json) {
        this(name, JsonUtil.getInstance().createArrayNode(json));
    }

    public JSONDataResource(String name, ArrayNode json) {
        super(name, json);
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
