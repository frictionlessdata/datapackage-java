package io.frictionlessdata.datapackage;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.util.JsonUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static io.frictionlessdata.datapackage.Validator.isValidUrl;

@JsonPropertyOrder({
	"title",
	"email",
	"path",
	"role",
	"organization"
})
public class Contributor {
	static final String invalidUrlMsg = "URLs for contributors must be fully qualified";
    private String title;
    private String email;
    private URL path;
    private Role role;
    private String organization;

    public String getTitle() {
		return title;
	}

	public String getEmail() {
		return email;
	}

	public URL getPath() {
		return path;
	}

	public Role getRole() {
		return role;
	}

	public String getOrganization() {
		return organization;
	}

	/**
     * Create a new Contributor object from a JSON representation
	 *
     * @param jsonObj JSON representation, eg. from Package definition
     * @return new Dialect object with values from JSONObject
     */
    public static Contributor fromJson(Map jsonObj) {
        if (null == jsonObj)
            return null;
        try {
        	Contributor c = JsonUtil.getInstance().convertValue(jsonObj, Contributor.class);
			if (c.path != null && !isValidUrl(c.path)) {
	        	throw new DataPackageException(invalidUrlMsg);
	        }
	        return c;
        } catch (Exception ex) {
        	Throwable cause = ex.getCause();
        	if (Objects.nonNull(cause) && cause.getClass().isAssignableFrom(InvalidFormatException.class)) {
        		if (Objects.nonNull(cause.getCause()) && cause.getCause().getClass().isAssignableFrom(MalformedURLException.class)) {
        			throw new DataPackageException(invalidUrlMsg);
        		}
        	} 
        	throw new DataPackageException(ex);
        }
    }
    
    /**
     * Create a new Contributor object from a JSON representation
     * @param jsonArr JSON representation, eg. from Package definition
     * @return new Dialect object with values from JSONObject
     */
    public static Collection<Contributor> fromJson(Collection<Map<String,?>> jsonArr) {
        final Collection<Contributor> contributors = new ArrayList<>();
        Iterator<Map<String, ?>> iter = jsonArr.iterator();
        while (iter.hasNext()) {
			Map obj = (Map) iter.next();
			contributors.add(fromJson(obj));
		}
        return contributors;
    }

    public static Collection<Contributor> fromJson(String json) {
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
