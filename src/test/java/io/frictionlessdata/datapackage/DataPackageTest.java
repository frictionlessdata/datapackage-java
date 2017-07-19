package io.frictionlessdata.datapackage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.everit.json.schema.ValidationException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.Assert;

/**
 *
 * 
 */
public class DataPackageTest {
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
   
    
    @Test
    public void testLoadFromJsonString() throws IOException{
        // Get path of source file:
        String sourceFileAbsPath = DataPackageTest.class.getResource("/fixtures/multi_data_datapackage.json").getPath();

        // Get string content version of source file.
        String jsonString = new String(Files.readAllBytes(Paths.get(sourceFileAbsPath)));
        
        // Create DataPackage instance from jsonString
        DataPackage dp = new DataPackage(jsonString);
        
        // Assert
        Assert.assertNotNull(dp);
    }
    
    @Test
    public void testLoadFromValidJsonObject(){
        // Create JSON Object for testing
        JSONObject jsonObject = new JSONObject("{\"name\": \"test\"}");
          
        // Build resources
        JSONObject resource1 = new JSONObject("{\"name\": \"first-resource\", \"path\": [\"foo.txt\", \"bar.txt\", \"baz.txt\"]}");
        JSONObject resource2 = new JSONObject("{\"name\": \"second-resource\", \"path\": [\"bar.txt\", \"baz.txt\"]}");
        
        List resourceArrayList = new ArrayList();
        resourceArrayList.add(resource1);
        resourceArrayList.add(resource2);
        
        JSONArray resources = new JSONArray(resourceArrayList);

        // Add the resources
        jsonObject.put("resources", resources);
        
        // Build the datapackage
        DataPackage dp = new DataPackage(jsonObject);
        
        // Assert
        Assert.assertNotNull(dp);
    }
    
    @Test
    public void testLoadInvalidJsonObject(){
        // Create JSON Object for testing
        JSONObject jsonObject = new JSONObject("{\"name\": \"test\"}");
        
        // Build the datapackage, it will throw ValidationException because there are no resources.
        exception.expect(ValidationException.class);
        DataPackage dp = new DataPackage(jsonObject);
    }
    

    @Test
    public void testLoadFromFileWhenPathDoesNotExist() throws FileNotFoundException {
        exception.expect(FileNotFoundException.class);
        DataPackage dp = new DataPackage("/this/path/does/not/exist", null);
    }
    
    @Test
    public void testLoadFromFileWhenPathExists() throws FileNotFoundException, IOException {
        // Get path of source file:
        String sourceFileAbsPath = DataPackageTest.class.getResource("/fixtures/multi_data_datapackage.json").getPath();

        // Get string content version of source file.
        String jsonString = new String(Files.readAllBytes(Paths.get(sourceFileAbsPath)));
        jsonString = jsonString.replace("\n", "").replace("\r", "").replace(" ", "");

        // Build DataPackage instance based on source file path.
        DataPackage dp = new DataPackage(sourceFileAbsPath, null);

        // We're not asserting the String value since the order of the JSONObject elements is not guaranteed.
        // Just compare the length of the String, should be enough.
        Assert.assertEquals(dp.getJSONObject().toString().length(), jsonString.length()); 
    }
    
    
    @Test
    public void testLoadFromFileWhenPathExistsButIsNotJson() throws FileNotFoundException{
        // Get path of source file:
        String sourceFileAbsPath = DataPackageTest.class.getResource("/fixtures/not_a_json_datapackage.json").getPath();
        
        exception.expect(JSONException.class);
        DataPackage dp = new DataPackage(sourceFileAbsPath, null);
    }
   
    
    @Test
    public void testValidUrl() throws MalformedURLException, IOException{
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/multi_data_datapackage.json");
        DataPackage dp = new DataPackage(url);
        Assert.assertNotNull(dp.getJSONObject());
    }
    
    @Test
    public void testValidUrlWithInvalidJson() throws MalformedURLException, IOException{
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/simple_invalid_datapackage.json");
        exception.expect(ValidationException.class);
        DataPackage dp = new DataPackage(url);
        
    }
    
    
    @Test
    public void testUrlDoesNotExist() throws MalformedURLException, IOException{
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/NON-EXISTANT-FOLDER/multi_data_datapackage.json");
        exception.expect(FileNotFoundException.class);
        DataPackage dp = new DataPackage(url);
    }
    

    
    //TODO: come up with attribute edit tests:
    // Examples here: https://github.com/frictionlessdata/datapackage-py/blob/master/tests/test_datapackage.py


}
