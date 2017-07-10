package io.frictionlessdata.datapackage;

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
    public void testValidatingInvalidJsonObject() {
        JSONObject datapackageJsonObject = new JSONObject("{\"invalid\" : \"json\"}");
        
        exception.expect(ValidationException.class);
        validator.validate(datapackageJsonObject);
        
    }
    
    @Test
    public void testValidatingInvalidJsonString(){
        String datapackageJsonString = "{\"invalid\" : \"json\"}";
        
        exception.expect(ValidationException.class);
        validator.validate(datapackageJsonString);   
    }
}
