package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

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
    public static Contributor fromJson(JSONObject jsonObj) throws MalformedURLException {
        if (null == jsonObj)
            return null;
        Contributor c = new Contributor();
        if (jsonObj.has("title"))
            c.title = jsonObj.getString("title");
        if (jsonObj.has("email"))
            c.title = jsonObj.getString("email");
        if (jsonObj.has("path")) {
            URL url = new URL(jsonObj.getString("path"));
            if (isValidUrl(url)) {
                c.path = url;
            } else {
                throw new DataPackageException("URLs for contributors must be fully qualified");
            }
        }
        if (jsonObj.has("role")) {
            String role = jsonObj.getString("role");
            c.role = Role.valueOf(role.toUpperCase());
        }
        if (jsonObj.has("organization"))
            c.title = jsonObj.getString("organization");
        return c;
    }

    /**
     * Create a new Contributor object from a JSON representation
     * @param jsonArr JSON representation, eg. from Package definition
     * @return new Dialect object with values from JSONObject
     */
    public static Collection<Contributor> fromJson(JSONArray jsonArr) throws MalformedURLException {
        final Collection<Contributor> contributors = new ArrayList<>();
        for (int cnt = 0; cnt < jsonArr.length(); cnt++) {
            JSONObject obj = jsonArr.getJSONObject(cnt);
            contributors.add(fromJson(obj));
        };
        return contributors;
    }

    public static Collection<Contributor> fromJson(String json) throws MalformedURLException {
        return fromJson(new JSONArray(json));
    }

    public static enum Role {
        AUTHOR,
        PUBLISHER,
        MAINTAINER,
        WRANGLER,
        CONTRIBUTOR
    }
}
