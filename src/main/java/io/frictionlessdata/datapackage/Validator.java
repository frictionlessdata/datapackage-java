package io.frictionlessdata.datapackage;
import java.io.InputStream;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 *
 * Validates against schema.
 */
public class Validator {
    
    //FIXME: File should most likely reside and be loaded locally instead.
    private final String SCHEMA_DATA_PACKAGE_JSON = "http://specs.frictionlessdata.io/schemas/data-package.json";
    
    /**
     * Validates a given JSON Object against the datapackage.json schema.
     * @param datapackageJsonObject
     * @throws ValidationException 
     */
    public void validate(JSONObject datapackageJsonObject) throws ValidationException{
        InputStream inputStream = getClass().getResourceAsStream(SCHEMA_DATA_PACKAGE_JSON);
        JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
        Schema schema = SchemaLoader.load(rawSchema);
        schema.validate(datapackageJsonObject); // throws a ValidationException if this object is invalid
    }
    
    /**
     * Validates a given JSON String against the datapackage.json schema.
     * @param datapackageJsonString
     * @throws ValidationException 
     */
    public void validate(String datapackageJsonString) throws ValidationException{
        JSONObject datapackageJsonObject = new JSONObject(datapackageJsonString);
        validate(datapackageJsonObject);
    }
}
