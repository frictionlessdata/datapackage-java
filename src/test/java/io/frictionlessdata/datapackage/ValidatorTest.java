package io.frictionlessdata.datapackage;

import com.fasterxml.jackson.databind.JsonNode;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.exception.ValidationException;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test calls for JSON Validator class.
 * 
 */
public class ValidatorTest {
    private static URL url;

    @BeforeAll
    public static void setup() throws MalformedURLException {
        url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/datapackages/multi-data/datapackage.json");
    }
    
    @Test
    @DisplayName("Test validating invalid JSON object as datapackage JSON throws an exception")
    public void testValidatingInvalidJsonObject() throws DataPackageException {
        JsonNode datapackageJsonObject = JsonUtil.getInstance().createNode("{\"invalid\" : \"json\"}");

        assertThrows(ValidationException.class, () -> {Validator.validate(datapackageJsonObject);});
    }
    
    @Test
    @DisplayName("Test validating invalid JSON string as datapackage JSON throws an exception")
    public void testValidatingInvalidJsonString() throws DataPackageException{
        String datapackageJsonString = "{\"invalid\" : \"json\"}";

        assertThrows(ValidationException.class, () -> {Validator.validate(datapackageJsonString);});
    }
    
    @Test
    @DisplayName("Test setting an undefined type of profile on a datapackage throws an exception")
    public void testValidationWithInvalidProfileId() throws Exception {
        Package dp = new Package(url, true);
        
        String invalidProfileId = "INVALID_PROFILE_ID";
        dp.setProfile(invalidProfileId);

        DataPackageException ex = assertThrows(DataPackageException.class, dp::validate);
        Assertions.assertEquals("Invalid profile id: " + invalidProfileId, ex.getMessage());
    }
    
    @Test
    @DisplayName("Test setting a profile from an URL on a datapackage")
    public void testValidationWithValidProfileUrl() throws Exception {
        Package dp = new Package(url,  true);
        dp.setProfile( "https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/main/resources/schemas/data-package.json");
        
        dp.validate();
        
        // No exception thrown, test passes.
        Assertions.assertEquals("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/" +
                "master/src/main/resources/schemas/data-package.json", dp.getProfile());
    }
    
    @Test
    @DisplayName("Test setting a profile from an invalid URL on a datapackage throws an exception")
    public void testValidationWithInvalidProfileUrl() throws Exception {
        Package dp = new Package(url, true);
        
        String invalidProfileUrl = "https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/main/resources/schemas/INVALID.json";
        dp.setProfile(invalidProfileUrl);

        DataPackageException ex = assertThrows(DataPackageException.class, dp::validate);
        Assertions.assertEquals("Invalid profile schema URL: " + invalidProfileUrl, ex.getMessage());
    }
}
