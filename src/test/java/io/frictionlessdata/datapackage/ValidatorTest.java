package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import java.io.IOException;
import org.everit.json.schema.ValidationException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test calls for JSON Validator class.
 * 
 */
public class ValidatorTest {
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    private Validator validator = null;
    
    @Before
    public void setup(){
        validator = new Validator();
    }
    
    @Test
    public void testValidatingInvalidJsonObject() throws IOException, DataPackageException {
        JSONObject datapackageJsonObject = new JSONObject("{\"invalid\" : \"json\"}");
        
        exception.expect(ValidationException.class);
        validator.validate(datapackageJsonObject);  
    }
    
    @Test
    public void testValidatingInvalidJsonString() throws IOException, DataPackageException{
        String datapackageJsonString = "{\"invalid\" : \"json\"}";
        
        exception.expect(ValidationException.class);
        validator.validate(datapackageJsonString);   
    }
}
