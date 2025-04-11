package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import io.frictionlessdata.tableschema.util.JsonUtil;

/**
 * A non-tabular resource that holds JSON object data
 */
@JsonInclude(value= JsonInclude.Include. NON_EMPTY, content= JsonInclude.Include. NON_NULL)
public class JSONObjectResource extends AbstractDataResource<ObjectNode> {

    public JSONObjectResource(String name, String json) {
        super(name, (ObjectNode)JsonUtil.getInstance().createNode(json));
        super.format = getResourceFormat();
    }

    public JSONObjectResource(String name, ObjectNode json) {
        super(name, json);
        super.format = getResourceFormat();
    }

    @Override
    String getResourceFormat() {
        return TableDataSource.Format.FORMAT_JSON.getLabel();
    }

    @Override
    public String getProfile() {
        return Profile.PROFILE_DATA_RESOURCE_DEFAULT;
    }
}
