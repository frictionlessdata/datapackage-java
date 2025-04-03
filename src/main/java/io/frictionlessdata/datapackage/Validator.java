package io.frictionlessdata.datapackage;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.ValidationMessage;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.exception.ValidationException;
import io.frictionlessdata.tableschema.schema.FormalSchemaValidator;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.apache.commons.validator.routines.UrlValidator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Set;

/**
 * Validates a package schema against the frictionlessdata table-schema.json (from the TableSchema project).
 */
public class Validator {

    /**
     * Validates a given JSON Object against the default profile schema.
     *
     * @param jsonObjectToValidate JSON Object to validate
     * @throws IOException         If an I/O error occurs
     * @throws DataPackageException If the profile id is invalid
     * @throws ValidationException If the JSON Object is invalid
     */
    public static void validate(JsonNode jsonObjectToValidate) throws IOException, DataPackageException, ValidationException {
        // If a profile value is provided.
        Set<ValidationMessage> errors;
        String profileId = Profile.PROFILE_DATA_PACKAGE_DEFAULT;
        if (jsonObjectToValidate.has(Package.JSON_KEY_PROFILE)) {
            profileId = jsonObjectToValidate.get(Package.JSON_KEY_PROFILE).asText();

            String[] schemes = {"http", "https"};
            UrlValidator urlValidator = new UrlValidator(schemes);

            if (urlValidator.isValid(profileId)) {
                errors = validate(jsonObjectToValidate, new URL(profileId));
            } else {
                errors = validate(jsonObjectToValidate, profileId);
            }
        } else {
            // If no profile value is provided, use default value.
            errors = validate(jsonObjectToValidate, Profile.PROFILE_DATA_PACKAGE_DEFAULT);
        }

        if (!errors.isEmpty()) {
            throw new ValidationException(profileId, errors);
        }
    }

    /**
     * Validates a given JSON Object against a given profile schema.
     *
     * @param jsonObjectToValidate
     * @param profileId
     * @throws DataPackageException
     * @throws ValidationException
     */
    private static Set<ValidationMessage> validate(JsonNode jsonObjectToValidate, String profileId) throws DataPackageException {
        InputStream inputStream = Validator.class.getResourceAsStream("/schemas/" + profileId + ".json");
        if (inputStream != null) {
            FormalSchemaValidator schema = FormalSchemaValidator.fromJson(inputStream);
            Set<ValidationMessage> errors = schema.validate(jsonObjectToValidate);// throws a ValidationException if this object is invalid
            return errors;
        } else {
            throw new DataPackageException("Invalid profile id: " + profileId);
        }
    }

    /**
     * @param jsonObjectToValidate
     * @param schemaUrl
     * @throws IOException
     * @throws DataPackageException
     * @throws ValidationException
     */
    private static Set<ValidationMessage> validate(JsonNode jsonObjectToValidate, URL schemaUrl) throws IOException, DataPackageException {
        try {
            InputStream inputStream = schemaUrl.openStream();
            FormalSchemaValidator schema = FormalSchemaValidator.fromJson(inputStream);
            Set<ValidationMessage> errors = schema.validate(jsonObjectToValidate);// throws a ValidationException if this object is invalid
            return errors;
        } catch (FileNotFoundException e) {
            throw new DataPackageException("Invalid profile schema URL: " + schemaUrl);
        }
    }

    /**
     * Validates a given JSON String against the default profile schema.
     *
     * @param jsonStringToValidate
     * @throws IOException
     * @throws DataPackageException
     * @throws ValidationException
     */
    public static void validate(String jsonStringToValidate) throws IOException, DataPackageException, ValidationException {
        JsonNode jsonObject = JsonUtil.getInstance().createNode(jsonStringToValidate);
        validate(jsonObject);
    }

    /**
     * Check whether an input URL is valid according to DataPackage specs.
     *
     * From the specification: "URLs MUST be fully qualified. MUST be using either
     * http or https scheme."
     *
     * https://frictionlessdata.io/specs/data-resource/#url-or-path
     *
     * @param url URL to test
     * @return true if the String contains a URL starting with HTTP/HTTPS
     */
    public static boolean isValidUrl(URL url) {
        return isValidUrl(url.toExternalForm());
    }

    /**
     * Check whether an input string contains a valid URL.
     *
     * From the specification: "URLs MUST be fully qualified. MUST be using either
     * http or https scheme."
     *
     * https://frictionlessdata.io/specs/data-resource/#url-or-path
     *
     * @param objString String to test
     * @return true if the String contains a URL starting with HTTP/HTTPS
     */
    public static boolean isValidUrl(String objString) {
        String[] schemes = {"http", "https"};
        UrlValidator urlValidator = new UrlValidator(schemes);

        return urlValidator.isValid(objString);
    }
}
