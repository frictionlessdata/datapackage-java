package io.frictionlessdata.datapackage;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.exception.JsonParsingException;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

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
    private String role;
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

	public String getRole() {
		return role;
	}

	public String getOrganization() {
		return organization;
	}

    public static Collection<Contributor> fromJson(JsonNode json) {
		if ((null == json) || json.isEmpty() || (!(json instanceof ArrayNode))) {
			return null;
		}

		try {
			return JsonUtil.getInstance().deserialize(json, new TypeReference<>() {});
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

	public static Collection<Contributor> fromJson(String json) {
		if (StringUtils.isEmpty(json)) {
			return null;
		}
		JsonNode jsonNode = JsonUtil.getInstance().readValue(json);
		return fromJson(jsonNode);
	}
}
