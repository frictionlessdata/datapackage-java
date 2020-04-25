package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.util.JsonUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static io.frictionlessdata.datapackage.Package.isValidUrl;

public class Contributor {
    private String title;
    private String email;
    private URL path;
    private Role role;
    private String organization;

    /**
     * Create a new Contributor object from a JSON representation
     * @param jsonObj JSON representation, eg. from Package definition
     * @return new Dialect object with values from JSONObject
     */
    public static Contributor fromJson(Map jsonObj) throws MalformedURLException {
        if (null == jsonObj)
            return null;
        Contributor c = new Contributor();
        if (jsonObj.containsKey("title"))
            c.title = jsonObj.get("title").toString();
        if (jsonObj.containsKey("email"))
            c.title = jsonObj.get("email").toString();
        if (jsonObj.containsKey("path")) {
            URL url = new URL(jsonObj.get("path").toString());
            if (isValidUrl(url)) {
                c.path = url;
            } else {
                throw new DataPackageException("URLs for contributors must be fully qualified");
            }
        }
        if (jsonObj.containsKey("role")) {
            String role = jsonObj.get("role").toString();
            c.role = Role.valueOf(role.toUpperCase());
        }
        if (jsonObj.containsKey("organization"))
            c.title = jsonObj.get("organization").toString();
        return c;
    }

    /**
     * Create a new Contributor object from a JSON representation
     * @param jsonArr JSON representation, eg. from Package definition
     * @return new Dialect object with values from JSONObject
     */
    public static Collection<Contributor> fromJson(Collection<Map<String,?>> jsonArr) throws MalformedURLException {
        final Collection<Contributor> contributors = new ArrayList<>();
        Iterator<Map<String, ?>> iter = jsonArr.iterator();
        while (iter.hasNext()) {
			Map obj = (Map) iter.next();
			contributors.add(fromJson(obj));
		}
        return contributors;
    }

    public static Collection<Contributor> fromJson(String json) throws MalformedURLException {
    	Collection<Map<String, ?>> objArray = new ArrayList<>();
    	JsonUtil.getInstance().createArrayNode(json).elements().forEachRemaining(o -> {
    		objArray.add(JsonUtil.getInstance().convertValue(o, Map.class));
    	});
        return fromJson(objArray);
    }

    public static enum Role {
        AUTHOR,
        PUBLISHER,
        MAINTAINER,
        WRANGLER,
        CONTRIBUTOR
    }
}
