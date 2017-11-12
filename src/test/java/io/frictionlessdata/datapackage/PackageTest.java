package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.PackageException;
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
public class PackageTest {
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
   
    
    @Test
    public void testLoadFromJsonString() throws IOException{

        // Create simple multi DataPackage from Json String
        Package dp = this.getSimpleMultiDataPackageFromString();
        
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
        Package dp = new Package(jsonObject);
        
        // Assert
        Assert.assertNotNull(dp);
    }
    
    @Test
    public void testLoadInvalidJsonObject(){
        // Create JSON Object for testing
        JSONObject jsonObject = new JSONObject("{\"name\": \"test\"}");
        
        // Build the datapackage, it will throw ValidationException because there are no resources.
        exception.expect(ValidationException.class);
        Package dp = new Package(jsonObject);
    }
    

    @Test
    public void testLoadFromFileWhenPathDoesNotExist() throws FileNotFoundException {
        exception.expect(FileNotFoundException.class);
        Package dp = new Package("/this/path/does/not/exist", null);
    }
    
    @Test
    public void testLoadFromFileWhenPathExists() throws FileNotFoundException, IOException {
        // Get path of source file:
        String sourceFileAbsPath = PackageTest.class.getResource("/fixtures/multi_data_datapackage.json").getPath();

        // Get string content version of source file.
        String jsonString = new String(Files.readAllBytes(Paths.get(sourceFileAbsPath)));
        jsonString = jsonString.replace("\n", "").replace("\r", "").replace(" ", "");

        // Build DataPackage instance based on source file path.
        Package dp = new Package(sourceFileAbsPath, null);

        // We're not asserting the String value since the order of the JSONObject elements is not guaranteed.
        // Just compare the length of the String, should be enough.
        Assert.assertEquals(dp.getJSONObject().toString().length(), jsonString.length());
        
    }
    
    @Test
    public void testLoadFromFileBasePath() throws FileNotFoundException, IOException {
        // Get path of source file:
        String sourceFileAbsPath = PackageTest.class.getResource("/fixtures/multi_data_datapackage.json").getPath();
        
        String relativePath = "multi_data_datapackage.json";
        String basePath = sourceFileAbsPath.replace("/" + relativePath, "");
        
        // Build DataPackage instance based on source file path.
        Package dp = new Package(relativePath, basePath);
        Assert.assertNotNull(dp.getJSONObject());
        
        // Check if base path was set properly;
        Assert.assertEquals(basePath, dp.getBasePath());
    }
    
    
    @Test
    public void testLoadFromFileWhenPathExistsButIsNotJson() throws FileNotFoundException{
        // Get path of source file:
        String sourceFileAbsPath = PackageTest.class.getResource("/fixtures/not_a_json_datapackage.json").getPath();
        
        exception.expect(JSONException.class);
        Package dp = new Package(sourceFileAbsPath, null);
    }
   
    
    @Test
    public void testValidUrl() throws MalformedURLException, IOException{
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/multi_data_datapackage.json");
        Package dp = new Package(url);
        Assert.assertNotNull(dp.getJSONObject());
    }
    
    @Test
    public void testValidUrlWithInvalidJson() throws MalformedURLException, IOException{
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/simple_invalid_datapackage.json");
        exception.expect(ValidationException.class);
        Package dp = new Package(url);
        
    }
    
    @Test
    public void testUrlDoesNotExist() throws MalformedURLException, IOException{
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/NON-EXISTANT-FOLDER/multi_data_datapackage.json");
        exception.expect(FileNotFoundException.class);
        Package dp = new Package(url);
    }
    
    @Test
    public void testGetResources() throws IOException{
        // Create simple multi DataPackage from Json String
        Package dp = this.getSimpleMultiDataPackageFromString();
        Assert.assertEquals(3, dp.getResources().length());
    }
    
    @Test
    public void testGetExistingResource() throws IOException{
        // Create simple multi DataPackage from Json String
        Package dp = this.getSimpleMultiDataPackageFromString();
        JSONObject resourceJsonObject = dp.getResource("third-resource");
        Assert.assertNotNull(resourceJsonObject);
    }
    
    @Test
    public void testGetNonExistingResource() throws IOException{
        // Create simple multi DataPackage from Json String
        Package dp = this.getSimpleMultiDataPackageFromString();
        JSONObject resourceJsonObject = dp.getResource("non-existing-resource");
        Assert.assertNull(resourceJsonObject);
    }
    
    @Test
    public void testRemoveResource() throws IOException{
        Package dp = this.getSimpleMultiDataPackageFromString();
        
        Assert.assertEquals(3, dp.getResources().length());
        dp.removeResource("second-resource");
        
        Assert.assertEquals(2, dp.getResources().length());
        dp.removeResource("third-resource");
        Assert.assertEquals(1, dp.getResources().length());
        
        dp.removeResource("third-resource");
        Assert.assertEquals(1, dp.getResources().length());
    }
    
    @Test
    public void testAddValidResource() throws PackageException, IOException{
        Package dp = this.getSimpleMultiDataPackageFromString();
        
        Assert.assertEquals(3, dp.getResources().length());
        dp.addResource(new JSONObject("{\"name\": \"new-resource\", \"path\": [\"foo.txt\", \"baz.txt\"]}"));
        Assert.assertEquals(4, dp.getResources().length());
        
        JSONObject resourceJsonObject = dp.getResource("new-resource");
        Assert.assertNotNull(resourceJsonObject);
    }
    
    @Test
    public void testAddInvalidResource() throws PackageException, IOException{
        Package dp = this.getSimpleMultiDataPackageFromString();
        exception.expect(PackageException.class);
        dp.addResource(new JSONObject("{}"));
    }
    
    @Test
    public void testAddDuplicateNameResource() throws PackageException, IOException{
        Package dp = this.getSimpleMultiDataPackageFromString();
        
        exception.expect(PackageException.class);
        dp.addResource(new JSONObject("{\"name\": \"third-resource\", \"path\": [\"foo.txt\", \"baz.txt\"]}"));
    }
    
    
    private Package getSimpleMultiDataPackageFromString() throws IOException{
        // Get path of source file:
        String sourceFileAbsPath = PackageTest.class.getResource("/fixtures/multi_data_datapackage.json").getPath();

        // Get string content version of source file.
        String jsonString = new String(Files.readAllBytes(Paths.get(sourceFileAbsPath)));
        
        // Create DataPackage instance from jsonString
        Package dp = new Package(jsonString);
        
        return dp;
    }    

    
    //TODO: come up with attribute edit tests:
    // Examples here: https://github.com/frictionlessdata/datapackage-py/blob/master/tests/test_datapackage.py


}
