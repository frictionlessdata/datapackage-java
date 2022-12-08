package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.databind.node.ArrayNode;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import io.frictionlessdata.tableschema.util.JsonUtil;

public class JSONDataResource<C> extends AbstractDataResource<ArrayNode,C> {

    public JSONDataResource(String name, String json) {
        super(name, JsonUtil.getInstance().createArrayNode(json));
        super.format = getResourceFormat();
    }

    @Override
    String getResourceFormat() {
        return TableDataSource.Format.FORMAT_JSON.getLabel();
    }
}
