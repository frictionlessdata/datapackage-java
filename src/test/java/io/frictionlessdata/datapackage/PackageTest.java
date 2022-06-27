package io.frictionlessdata.datapackage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.frictionlessdata.datapackage.beans.EmployeeBean;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.exceptions.DataPackageFileOrUrlNotFoundException;
import io.frictionlessdata.datapackage.resource.FilebasedResource;
import io.frictionlessdata.datapackage.resource.JSONDataResource;
import io.frictionlessdata.datapackage.resource.Resource;
import io.frictionlessdata.datapackage.resource.ResourceTest;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.exception.JsonParsingException;
import io.frictionlessdata.tableschema.exception.ValidationException;
import io.frictionlessdata.tableschema.field.DateField;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static io.frictionlessdata.datapackage.TestUtil.getBasePath;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 *
 * 
 */
public class PackageTest {
    private static URL validUrl;
    static String resource1String = "{\"name\": \"first-resource\", \"path\": " +
            "[\"data/cities.csv\", \"data/cities2.csv\", \"data/cities3.csv\"]}";
    static String resource2String = "{\"name\": \"second-resource\", \"path\": " +
            "[\"data/area.csv\", \"data/population.csv\"]}";

    static ArrayNode testResources = JsonUtil.getInstance().createArrayNode(String.format("[%s,%s]", resource1String, resource2String));

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();


    @Before
    public void setup() throws MalformedURLException {
        validUrl = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/datapackages/multi-data/datapackage.json");
    }

    @Test
    public void testLoadFromJsonString() throws Exception {

        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(true);
        
        // Assert
        Assert.assertNotNull(dp);
    }
    
    @Test
    public void testLoadFromValidJsonNode() throws Exception {
        // Create Object for testing
        Map<String, Object> testMap = createTestMap();
        
        // Add the resources
        testMap.put("resources", testResources);
        
        // convert the object to a json string and Build the datapackage
        Package dp = new Package(asString(testMap), getBasePath(), true);
        
        // Assert
        Assert.assertNotNull(dp);
    }


    @Test
    public void testLoadFromValidJsonNodeWithInvalidResources() throws Exception {
        // Create JSON Object for testing
        Map<String, Object> testObj = createTestMap();

        // Build resources
        JsonNode res1 = createNode("{\"name\": \"first-resource\", \"path\": [\"foo.txt\", \"bar.txt\", \"baz.txt\"]}");
        JsonNode res2 = createNode("{\"name\": \"second-resource\", \"path\": [\"bar.txt\", \"baz.txt\"]}");

        List<JsonNode> resourceArrayList = new ArrayList<>();
        resourceArrayList.add(res1);
        resourceArrayList.add(res2);

        // Add the resources
        testObj.put("resources", resourceArrayList);

        // Build the datapackage
        Package dp = new Package(asString(testObj), getBasePath(), false);
        // Resolve the Resources -> FileNotFoundException due to non-existing files
        exception.expect(FileNotFoundException.class);
        List<Table> tables = dp.getResource("first-resource").getTables();
    }

    @Test
    public void testLoadInvalidJsonNode() throws Exception {
        // Create JSON Object for testing
        Map<String, Object> testObj = createTestMap();
        
        // Build the datapackage, it will throw ValidationException because there are no resources.
        exception.expect(DataPackageException.class);
        Package dp = new Package(asString(testObj), getBasePath(), true);
    }
    
    @Test
    public void testLoadInvalidJsonNodeNoStrictValidation() throws Exception {
        // Create JSON Object for testing
        Map<String, Object> testObj = createTestMap();
        
        // Build the datapackage, no strict validation by default
        Package dp = new Package(asString(testObj), getBasePath(), false);
        
        // Assert
        Assert.assertNotNull(dp);
    }

    @Test
    public void testLoadFromFileWhenPathDoesNotExist() throws Exception {
        exception.expect(DataPackageFileOrUrlNotFoundException.class);
        new Package(new File("/this/path/does/not/exist").toPath(), true);
    }
    
    @Test
    public void testLoadFromFileWhenPathExists() throws Exception {
        String fName = "/fixtures/multi_data_datapackage.json";
        // Get path of source file:
        String sourceFileAbsPath = PackageTest.class.getResource(fName).getPath();

        // Get string content version of source file.
        String jsonString = getFileContents(fName);
   
        // Build DataPackage instance based on source file path.
        Package dp = new Package(new File(sourceFileAbsPath).toPath(), true);

        // We're not asserting the String value since the order of the JsonNode elements is not guaranteed.
        // Just compare the length of the String, should be enough.
        JsonNode obj = createNode(dp.getJson());
        // a default 'profile' is being set, so the two packages will differ, unless a profile is added to the fixture data
        Assert.assertEquals(obj.get("resources").size(), createNode(jsonString).get("resources").size());
        Assert.assertEquals(obj.get("name"), createNode(jsonString).get("name"));
    }
    
    @Test
    public void testLoadFromFileBasePath() throws Exception {
        String pathSegment =  "/fixtures";
        String sourceFileName = "multi_data_datapackage.json";
        String pathName = pathSegment + "/" +sourceFileName;
        // Get path of source file:
        Path sourceFileAbsPath = Paths.get(PackageTest.class.getResource(pathName).toURI());
        Path basePath = sourceFileAbsPath.getParent();
        
        // Build DataPackage instance based on source file path.
        Package dp = new Package(new File(basePath.toFile(), sourceFileName).toPath(), true);
        Assert.assertNotNull(dp.getJson());
        
        // Check if base path was set properly;
        Assert.assertEquals(basePath, dp.getBasePath());
    }
    
    
    @Test
    public void testLoadFromFileWhenPathExistsButIsNotJson() throws Exception {
        // Get path of source file:
        String sourceFileAbsPath = PackageTest.class.getResource("/fixtures/not_a_json_datapackage.json").getPath();
        
        exception.expect(JsonParsingException.class);
        Package dp = new Package(sourceFileAbsPath, getBasePath(), true);
    }
   
    
    @Test
    public void testValidUrl() throws Exception {
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804

        Package dp = new Package(validUrl, true);
        Assert.assertNotNull(dp.getJson());
    }
    
    @Test
    public void testValidUrlWithInvalidJson() throws Exception {
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/simple_invalid_datapackage.json");
        exception.expect(DataPackageException.class);
        Package dp = new Package(url, true);
        
    }
    
    @Test
    public void testValidUrlWithInvalidJsonNoStrictValidation() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/simple_invalid_datapackage.json");
        
        Package dp = new Package(url, false);
        Assert.assertNotNull(dp.getJson());
    }
    
    @Test
    public void testUrlDoesNotExist() throws Exception {
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/NON-EXISTANT-FOLDER/multi_data_datapackage.json");
        exception.expect(DataPackageException.class);
        Package dp = new Package(url, true);
    }
    
    @Test
    public void testLoadFromJsonFileResourceWithStrictValidationForInvalidNullPath() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/invalid_multi_data_datapackage.json");
        
        exception.expectMessage("Invalid Resource. The path property or the data and format properties cannot be null.");
        Package dp = new Package(url, true);
    }
    
    @Test
    public void testLoadFromJsonFileResourceWithoutStrictValidationForInvalidNullPath() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/invalid_multi_data_datapackage.json");
        
        Package dp = new Package(url, false);
        Assert.assertEquals("Invalid Resource. The path property or the data and " +
                "format properties cannot be null.", dp.getErrors().get(0).getMessage());
    }
    
    @Test
    public void testCreatingResourceWithInvalidPathNullValue() throws Exception {
        exception.expectMessage("Invalid Resource. " +
                "The path property cannot be null for file-based Resources.");
        FilebasedResource resource = FilebasedResource.fromSource("resource-name", null, null);
        Assert.assertNotNull(resource);
    }

    
    @Test
    public void testGetResources() throws Exception {
        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(true);
        Assert.assertEquals(5, dp.getResources().size());
    }
    
    @Test
    public void testGetExistingResource() throws Exception {
        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(true);
        Resource resource = dp.getResource("third-resource");
        Assert.assertNotNull(resource);
    }


    @Test
    public void testReadTabseparatedResource() throws Exception {
        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(
                "/fixtures/tab_separated_datapackage.json", true);
        Resource resource = dp.getResource("first-resource");
        Dialect dialect = new Dialect();
        dialect.setDelimiter("\t");
        resource.setDialect(dialect);
        Assert.assertNotNull(resource);
        List<Object[]>data = resource.getData(false, false, false, false);
        Assert.assertEquals( 6, data.size());
        Assert.assertEquals("libreville", data.get(0)[0]);
        Assert.assertEquals("0.41,9.29", data.get(0)[1]);
        Assert.assertEquals( "dakar", data.get(1)[0]);
        Assert.assertEquals("14.71,-17.53", data.get(1)[1]);
        Assert.assertEquals("ouagadougou", data.get(2)[0]);
        Assert.assertEquals("12.35,-1.67", data.get(2)[1]);
        Assert.assertEquals("barranquilla", data.get(3)[0]);
        Assert.assertEquals("10.98,-74.88", data.get(3)[1]);
        Assert.assertEquals("cuidad de guatemala", data.get(5)[0]);
        Assert.assertEquals("14.62,-90.56", data.get(5)[1]);

    }

    @Test
    public void testReadTabseparatedResourceAndDialect() throws Exception {
        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(
                "/fixtures/tab_separated_datapackage_with_dialect.json", true);
        Resource resource = dp.getResource("first-resource");
        Assert.assertNotNull(resource);
        List<Object[]>data = resource.getData(false, false, false, false);
        Assert.assertEquals( 6, data.size());
        Assert.assertEquals("libreville", data.get(0)[0]);
        Assert.assertEquals("0.41,9.29", data.get(0)[1]);
        Assert.assertEquals( "dakar", data.get(1)[0]);
        Assert.assertEquals("14.71,-17.53", data.get(1)[1]);
        Assert.assertEquals("ouagadougou", data.get(2)[0]);
        Assert.assertEquals("12.35,-1.67", data.get(2)[1]);
        Assert.assertEquals("barranquilla", data.get(3)[0]);
        Assert.assertEquals("10.98,-74.88", data.get(3)[1]);
        Assert.assertEquals("cuidad de guatemala", data.get(5)[0]);
        Assert.assertEquals("14.62,-90.56", data.get(5)[1]);
    }


    @Test
    public void testGetNonExistingResource() throws Exception {
        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(true);
        Resource resource = dp.getResource("non-existing-resource");
        Assert.assertNull(resource);
    }
    
    @Test
    public void testRemoveResource() throws Exception {
        Package dp = this.getDataPackageFromFilePath(true);
        
        Assert.assertEquals(5, dp.getResources().size());
        
        dp.removeResource("second-resource");
        Assert.assertEquals(4, dp.getResources().size());
        
        dp.removeResource("third-resource");
        Assert.assertEquals(3, dp.getResources().size());
        
        dp.removeResource("third-resource");
        Assert.assertEquals(3, dp.getResources().size());
    }
    
    @Test
    public void testAddValidResource() throws Exception{
        String pathName = "/fixtures/multi_data_datapackage.json";
        Package dp = this.getDataPackageFromFilePath(pathName,true);
        Path sourceFileAbsPath = Paths.get(PackageTest.class.getResource(pathName).toURI());
        String basePath = sourceFileAbsPath.getParent().toString();
        Assert.assertEquals(5, dp.getResources().size());

        List<File> files = new ArrayList<>();
        for (String s : Arrays.asList("cities.csv", "cities2.csv")) {
            files.add(new File(s));
        }
        Resource resource = Resource.build("new-resource", files, basePath);
        Assert.assertTrue(resource instanceof FilebasedResource);
        dp.addResource((FilebasedResource)resource);
        Assert.assertEquals(6, dp.getResources().size());
        
        Resource gotResource = dp.getResource("new-resource");
        Assert.assertNotNull(gotResource);
    }
    
    @Test
    public void testCreateInvalidJSONResource() throws Exception {
        Package dp = this.getDataPackageFromFilePath(true);

        exception.expectMessage("Invalid Resource, it does not have a name property.");
        Resource res = new JSONDataResource(null, testResources.toString());
        dp.addResource(res);
    }


    @Test
    public void testAddDuplicateNameResourceWithStrictValidation() throws Exception {
        String pathName = "/fixtures/multi_data_datapackage.json";
        Package dp = this.getDataPackageFromFilePath(pathName, true);
        Path sourceFileAbsPath = Paths.get(PackageTest.class.getResource(pathName).toURI());
        String basePath = sourceFileAbsPath.getParent().toString();


        List<File> files = new ArrayList<>();
        for (String s : Arrays.asList("cities.csv", "cities2.csv")) {
            files.add(new File(s));
        }
        Resource resource = Resource.build("third-resource", files, basePath);
        Assert.assertTrue(resource instanceof FilebasedResource);
        
        exception.expectMessage("A resource with the same name already exists.");
        dp.addResource((FilebasedResource)resource);
    }
    
    @Test
    public void testAddDuplicateNameResourceWithoutStrictValidation() throws Exception{
        String pathName = "/fixtures/multi_data_datapackage.json";
        Package dp = this.getDataPackageFromFilePath(pathName, false);
        Path sourceFileAbsPath = Paths.get(PackageTest.class.getResource(pathName).toURI());
        String basePath = sourceFileAbsPath.getParent().toString();

        List<File> files = new ArrayList<>();
        for (String s : Arrays.asList("cities.csv", "cities2.csv")) {
            files.add(new File(s));
        }
        Resource resource = Resource.build("third-resource", files, basePath);
        Assert.assertTrue(resource instanceof FilebasedResource);
        dp.addResource((FilebasedResource)resource);
        
        Assert.assertEquals(1, dp.getErrors().size());
        Assert.assertEquals("A resource with the same name already exists.", dp.getErrors().get(0).getMessage());
    }
    
    
    @Test
    public void testSaveToJsonFile() throws Exception{
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        
        Package savedPackage = this.getDataPackageFromFilePath(true);
        savedPackage.write(tempDirPath.toFile(), false);

        Package readPackage = new Package(tempDirPath.resolve(Package.DATAPACKAGE_FILENAME),false);
        JsonNode readPackageJson = createNode(readPackage.getJson()) ;
        JsonNode savedPackageJson = createNode(savedPackage.getJson()) ;
        Assert.assertTrue(readPackageJson.equals(savedPackageJson));
    }
    
    @Test
    public void testSaveToAndReadFromZipFile() throws Exception{
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        File createdFile = new File(tempDirPath.toFile(), "test_save_datapackage.zip");
        
        // saveDescriptor the datapackage in zip file.
        Package originalPackage = this.getDataPackageFromFilePath(true);
        originalPackage.write(createdFile, true);
        
        // Read the datapckage we just saved in the zip file.
        Package readPackage = new Package(createdFile.toPath(), false);
        
        // Check if two data packages are have the same key/value pairs.
        String expected = readPackage.getJson();
        String actual = originalPackage.getJson();
        Assert.assertEquals(expected, actual);
    }


    // the `datapackage.json` is in a folder `countries-and-currencies` on the top
    // level of the zip file.
    @Test
    public void testReadFromZipFileWithDirectoryHierarchy() throws Exception{
        String[] usdTestData = new String[]{"USD", "US Dollar", "$"};
        String[] gbpTestData = new String[]{"GBP", "Pound Sterling", "£"};
        String sourceFileAbsPath = ResourceTest.class.getResource("/fixtures/zip/countries-and-currencies.zip").getPath();

        Package dp = new Package(new File(sourceFileAbsPath).toPath(), true);
        Resource r = dp.getResource("currencies");

        List<Object[]> data = r.getData(false, false, false, false);
        Assert.assertEquals(2, data.size());
        Assert.assertArrayEquals(usdTestData, data.get(0));
        Assert.assertArrayEquals(gbpTestData, data.get(1));
    }

    /*
     * ensure the zip file is closed after reading so we don't leave file handles dangling.
     */
    @Test
    public void testClosesZipFile() throws Exception{
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        File createdFile = new File(tempDirPath.toFile(), "test_save_datapackage.zip");
        Path resourcePath = TestUtil.getResourcePath("/fixtures/zip/countries-and-currencies.zip");
        Files.copy(resourcePath, createdFile.toPath());

        Package dp = new Package(createdFile.toPath(), true);
        Resource r = dp.getResource("currencies");
        createdFile.delete();
        Assert.assertFalse(createdFile.exists());
    }

    // Archive file name doesn't end with ".zip"
    @Test
    public void testReadFromZipFileWithDifferentSuffix() throws Exception{
        String[] usdTestData = new String[]{"USD", "US Dollar", "$"};
        String[] gbpTestData = new String[]{"GBP", "Pound Sterling", "£"};
        String sourceFileAbsPath = ResourceTest.class.getResource("/fixtures/zip/countries-and-currencies.zap").getPath();

        Package dp = new Package(new File(sourceFileAbsPath).toPath(), true);
        Resource r = dp.getResource("currencies");

        List<Object[]> data = r.getData(false, false, false, false);
        Assert.assertEquals(2, data.size());
        Assert.assertArrayEquals(usdTestData, data.get(0));
        Assert.assertArrayEquals(gbpTestData, data.get(1));
    }

    @Test
    public void testReadFromZipFileWithInvalidDatapackageFilenameInside() throws Exception{
        String sourceFileAbsPath = PackageTest.class.getResource("/fixtures/zip/invalid_filename_datapackage.zip").getPath();
        
        exception.expect(DataPackageException.class);
        new Package(new File(sourceFileAbsPath).toPath(), false);
    }
    
    @Test
    public void testReadFromZipFileWithInvalidDatapackageDescriptorAndStrictValidation() throws Exception{
        Path sourceFileAbsPath = Paths
                .get(PackageTest.class.getResource("/fixtures/zip/invalid_datapackage.zip").toURI());
        
        exception.expect(DataPackageException.class);
        new Package(sourceFileAbsPath.toFile().toPath(), true);
    }
    
    @Test
    public void testReadFromInvalidZipFilePath() throws Exception{
        exception.expect(DataPackageFileOrUrlNotFoundException.class);
        File invalidFile = new File ("/invalid/path/does/not/exist/datapackage.zip");
        Package p = new Package(invalidFile.toPath(), false);
    }

    
    @Test
    public void testMultiPathIterationForLocalFiles() throws Exception{
        Package pkg = this.getDataPackageFromFilePath(true);
        Resource resource = pkg.getResource("first-resource");
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getAllCityData();
        
        // Get Iterator.
        Iterator<String[]> iter = resource.objectArrayIterator();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String location = record[1];
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], location);
            
            expectedDataIndex++;
        } 
    }
    
    @Test
    public void testMultiPathIterationForRemoteFile() throws Exception{
        Package pkg = this.getDataPackageFromFilePath(true);
        Resource resource = pkg.getResource("second-resource");
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getAllCityData();
        
        // Get Iterator.
        Iterator<String[]> iter = resource.objectArrayIterator();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String location = record[1];
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], location);
            
            expectedDataIndex++;
        } 
    }
    
    @Test
    public void testResourceSchemaDereferencingForLocalDataFileAndRemoteSchemaFile() throws Exception {
        Package pkg = this.getDataPackageFromFilePath(true);
        Resource resource = pkg.getResource("third-resource");

        // Get string content version of the schema file.
        String schemaJsonString =getFileContents("/fixtures/schema/population_schema.json");

        Schema expectedSchema = Schema.fromJson(schemaJsonString, true);
        Assert.assertEquals(expectedSchema, resource.getSchema());

        // Get JSON Object
        JsonNode expectedSchemaJson = createNode(expectedSchema.getJson());
        JsonNode testSchemaJson = createNode(resource.getSchema().getJson());
        // Compare JSON objects
        Assert.assertTrue("Schemas don't match", expectedSchemaJson.equals(testSchemaJson));
    }
    
    @Test
    public void testResourceSchemaDereferencingForRemoteDataFileAndLocalSchemaFile() throws Exception {
        Package pkg = this.getDataPackageFromFilePath(true);
        Resource resource = pkg.getResource("fourth-resource");

        // Get string content version of the schema file.
        String schemaJsonString =getFileContents("/fixtures/schema/population_schema.json");

        Schema expectedSchema = Schema.fromJson(schemaJsonString, true);
        Assert.assertEquals(expectedSchema, resource.getSchema());

        // Get JSON Object
        JsonNode expectedSchemaJson = createNode(expectedSchema.getJson());
        JsonNode testSchemaJson = createNode(resource.getSchema().getJson());
        // Compare JSON objects
        Assert.assertEquals("Schemas don't match", expectedSchemaJson, testSchemaJson);
    }

    @Test
    public void testAddPackageProperty() throws Exception{
        Object[] entries = new Object[]{"K", 3.2, 2};
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("time", "s");
        props.put("length", 3.2);
        props.put("count", 7);

        Path pkgFile =  TestUtil.getResourcePath("/fixtures/datapackages/employees/datapackage.json");
        Package p = new Package(pkgFile, false);

        p.setProperty("mass unit", "kg");
        p.setProperty("mass flow", 3.2);
        p.setProperty("number of parcels", 5);
        p.setProperty("entries", entries);
        p.setProperty("props", props);
        p.setProperty("null", null);
        Assert.assertEquals("JSON doesn't match", "kg", p.getProperty("mass unit"));
        Assert.assertEquals("JSON doesn't match", new BigDecimal("3.2"), p.getProperty("mass flow"));
        Assert.assertEquals("JSON doesn't match", new BigInteger("5"), p.getProperty("number of parcels"));
        Assert.assertEquals("JSON doesn't match", Arrays.asList(entries), p.getProperty("entries"));
        Assert.assertEquals("JSON doesn't match", props, p.getProperty("props"));
        Assert.assertNull("JSON doesn't match", p.getProperty("null"));
    }

    @Test
    // tests the setProperties() method of Package
    public void testSetPackageProperties() throws Exception{
        Object[] entries = new Object[]{"K", 3.2, 2};
        Map<String, Object> map = new LinkedHashMap<>();
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("time", "s");
        props.put("length", 3.2);
        props.put("count", 7);
        map.put("mass unit", "kg");
        map.put("mass flow", 3.2);
        map.put("number of parcels", 5);
        map.put("entries", entries);
        map.put("props", props);
        map.put("null", null);
       
        Path pkgFile =  TestUtil.getResourcePath("/fixtures/datapackages/employees/datapackage.json");
        Package p = new Package(pkgFile, false);
        p.setProperties(map);
        Assert.assertEquals("JSON doesn't match", "kg", p.getProperty("mass unit"));
        Assert.assertEquals("JSON doesn't match", new BigDecimal("3.2"), p.getProperty("mass flow"));
        Assert.assertEquals("JSON doesn't match", new BigInteger("5"), p.getProperty("number of parcels"));
        Assert.assertEquals("JSON doesn't match", Arrays.asList(entries), p.getProperty("entries"));
        Assert.assertEquals("JSON doesn't match", props, p.getProperty("props"));
        Assert.assertNull("JSON doesn't match", p.getProperty("null"));
    }

    // test schema validation. Schema is invalid, must throw
    @Test
    public void testResourceSchemaDereferencingWithInvalidResourceSchema() {
        assertThrows(ValidationException.class, () -> this.getDataPackageFromFilePath(
                "/fixtures/multi_data_datapackage_with_invalid_resource_schema.json", true
        ));
    }
    
    @Test
    public void testResourceDialectDereferencing() throws Exception {
        Package pkg = this.getDataPackageFromFilePath(true);
        
        Resource resource = pkg.getResource("fifth-resource");

        // Get string content version of the dialect file.
        String dialectJsonString =getFileContents("/fixtures/dialect.json");
        
        // Compare.
        Assert.assertEquals(Dialect.fromJson(dialectJsonString), resource.getDialect());
    }

    @Test
    public void testAdditionalProperties() throws Exception {
        String fName = "/fixtures/datapackages/additional-properties/datapackage.json";
        String sourceFileAbsPath = PackageTest.class.getResource(fName).getPath();
        Package dp = new Package(new File(sourceFileAbsPath).toPath(), true);

        Object creator = dp.getProperty("creator");
        Assert.assertNotNull(creator);
        Assert.assertEquals("Horst", creator);

        Object testprop = dp.getProperty("testprop");
        Assert.assertNotNull(testprop);
        Assert.assertTrue(testprop instanceof Map);

        Object testarray = dp.getProperty("testarray");
        Assert.assertNotNull(testarray);
        Assert.assertTrue(testarray instanceof ArrayList);

        Object resObj = dp.getProperty("something");
        Assert.assertNull(resObj);
    }

    @Test
    public void testBeanResource1() throws Exception {
        Package pkg = new Package(new File( getBasePath().toFile(), "datapackages/bean-iterator/datapackage.json").toPath(), true);

        Resource resource = pkg.getResource("employee-data");
        final List<EmployeeBean> employees = resource.getData(EmployeeBean.class);
        Assert.assertEquals(3, employees.size());
        EmployeeBean frank = employees.get(1);
        Assert.assertEquals("Frank McKrank", frank.getName());
        Assert.assertEquals("1992-02-14", new DateField("date").formatValueAsString(frank.getDateOfBirth(), null, null));
        Assert.assertFalse(frank.getAdmin());
        Assert.assertEquals("(90.0, 45.0, NaN)", frank.getAddressCoordinates().toString());
        Assert.assertEquals("PT15M", frank.getContractLength().toString());
        Map info = frank.getInfo();
        Assertions.assertEquals(45, info.get("pin"));
        Assertions.assertEquals(83.23, info.get("rate"));
        Assertions.assertEquals(90, info.get("ssn"));
    }

    private Package getDataPackageFromFilePath(String datapackageFilePath, boolean strict) throws Exception {
        // Get string content version of source file.
        String jsonString = getFileContents(datapackageFilePath);
        
        // Create DataPackage instance from jsonString
        Package dp = new Package(jsonString, getBasePath(), strict);
        
        return dp;
    } 
    
    private Package getDataPackageFromFilePath(boolean strict) throws Exception {
        return this.getDataPackageFromFilePath("/fixtures/multi_data_datapackage.json", strict);
    }

    private static String getFileContents(String fileName) {
        try {
            // Create file-URL of source file:
            URL sourceFileUrl = PackageTest.class.getResource(fileName);
            // Get path of URL
            Path path = Paths.get(sourceFileUrl.toURI());
            return new String(Files.readAllBytes(path));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private List<String[]> getAllCityData(){
        List<String[]> expectedData  = new ArrayList<>();
        expectedData.add(new String[]{"libreville", "0.41,9.29"});
        expectedData.add(new String[]{"dakar", "14.71,-17.53"});
        expectedData.add(new String[]{"ouagadougou", "12.35,-1.67"});
        expectedData.add(new String[]{"barranquilla", "10.98,-74.88"});
        expectedData.add(new String[]{"rio de janeiro", "-22.91,-43.72"});
        expectedData.add(new String[]{"cuidad de guatemala", "14.62,-90.56"});
        expectedData.add(new String[]{"london", "51.50,-0.11"});
        expectedData.add(new String[]{"paris", "48.85,2.30"});
        expectedData.add(new String[]{"rome", "41.89,12.51"});
        
        return expectedData;
    }
    
    private static JsonNode createNode(String json) {
    	return JsonUtil.getInstance().createNode(json);
    }
    
    private Map<String, Object> createTestMap(){
    	HashMap<String, Object> map = new HashMap<>();
    	map.put("name", "test");
    	return map;
    }
    
    private String asString(Object object) {
    	return JsonUtil.getInstance().serialize(object);
    }
    
    //TODO: come up with attribute edit tests:
    // Examples here: https://github.com/frictionlessdata/datapackage-py/blob/master/tests/test_datapackage.py
}
