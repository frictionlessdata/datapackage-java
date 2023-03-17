package io.frictionlessdata.datapackage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.frictionlessdata.datapackage.beans.EmployeeBean;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.exceptions.DataPackageFileOrUrlNotFoundException;
import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.datapackage.resource.*;
import io.frictionlessdata.tableschema.exception.ValidationException;
import io.frictionlessdata.tableschema.field.DateField;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static io.frictionlessdata.datapackage.Profile.*;
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


    @BeforeAll
    public static void setup() throws MalformedURLException {
        validUrl = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/datapackages/multi-data/datapackage.json");
    }

    @Test
    public void testLoadFromJsonString() throws Exception {

        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(true);
        
        // Assert
        Assertions.assertNotNull(dp);
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
        Assertions.assertNotNull(dp);
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
        assertThrows(FileNotFoundException.class, () -> dp.getResource("first-resource").getTables());
    }

    @Test
    public void testLoadInvalidJsonNode() throws Exception {
        // Create JSON Object for testing
        Map<String, Object> testObj = createTestMap();
        
        // Build the datapackage, it will throw ValidationException because there are no resources.
        DataPackageValidationException ex = assertThrows(
                DataPackageValidationException.class,
                () -> new Package(asString(testObj), getBasePath(), true));
        Assertions.assertEquals("Trying to create a DataPackage from JSON, but no resource entries found", ex.getMessage());
    }
    
    @Test
    public void testLoadInvalidJsonNodeNoStrictValidation() throws Exception {
        // Create JSON Object for testing
        Map<String, Object> testObj = createTestMap();
        
        // Build the datapackage, no strict validation by default
        Package dp = new Package(asString(testObj), getBasePath(), false);
        
        // Assert
        Assertions.assertNotNull(dp);
    }

    @Test
    public void testLoadFromFileWhenPathDoesNotExist() throws Exception {
        DataPackageFileOrUrlNotFoundException ex = assertThrows(
                DataPackageFileOrUrlNotFoundException.class,
                () -> new Package(new File("/this/path/does/not/exist").toPath(), true));

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
        Assertions.assertEquals(obj.get("resources").size(), createNode(jsonString).get("resources").size());
        Assertions.assertEquals(obj.get("name"), createNode(jsonString).get("name"));
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
        Assertions.assertNotNull(dp.getJson());
        
        // Check if base path was set properly;
        Assertions.assertEquals(basePath, dp.getBasePath());
    }
    
    
    @Test
    public void testLoadFromFileWhenPathExistsButIsNotJson() throws Exception {
        // Get path of source file:
        String sourceFileAbsPath = PackageTest.class.getResource("/fixtures/not_a_json_datapackage.json").getPath();
        DataPackageException ex = assertThrows(
                DataPackageException.class,
                () -> new Package(sourceFileAbsPath, getBasePath(), true));
    }
   
    
    @Test
    public void testValidUrl() throws Exception {
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804

        Package dp = new Package(validUrl, true);
        Assertions.assertNotNull(dp.getJson());
    }
    
    @Test
    public void testValidUrlWithInvalidJson() throws Exception {
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/simple_invalid_datapackage.json");
        DataPackageException ex = assertThrows(
                DataPackageException.class,
                () -> new Package(url, true));
        
    }
    
    @Test
    public void testValidUrlWithInvalidJsonNoStrictValidation() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/simple_invalid_datapackage.json");
        
        Package dp = new Package(url, false);
        Assertions.assertNotNull(dp.getJson());
    }
    
    @Test
    public void testUrlDoesNotExist() throws Exception {
        // Preferably we would use mockito/powermock to mock URL Connection
        // But could not resolve AbstractMethodError: https://stackoverflow.com/a/32696152/4030804
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/NON-EXISTANT-FOLDER/multi_data_datapackage.json");
        DataPackageException ex = assertThrows(
                DataPackageException.class,
                () -> new Package(url, true));
    }
    
    @Test
    public void testLoadFromJsonFileResourceWithStrictValidationForInvalidNullPath() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/invalid_multi_data_datapackage.json");

        DataPackageValidationException ex = assertThrows(
                DataPackageValidationException.class,
                () -> new Package(url, true));
        Assertions.assertEquals("Invalid Resource. The path property or the data and format properties cannot be null.", ex.getMessage());
    }
    
    @Test
    public void testLoadFromJsonFileResourceWithoutStrictValidationForInvalidNullPath() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/invalid_multi_data_datapackage.json");
        
        Package dp = new Package(url, false);
        Assertions.assertEquals("Invalid Resource. The path property or the data and " +
                "format properties cannot be null.", dp.getErrors().get(0).getMessage());
    }
    
    @Test
    public void testCreatingResourceWithInvalidPathNullValue() throws Exception {
        DataPackageException ex = assertThrows(
                DataPackageException.class,
                () -> {FilebasedResource.fromSource(
                        "resource-name",
                        null,
                        null,
                        TableDataSource.getDefaultEncoding());});
        Assertions.assertEquals("Invalid Resource. " +
                "The path property cannot be null for file-based Resources.", ex.getMessage());

    }

    
    @Test
    public void testGetResources() throws Exception {
        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(true);
        Assertions.assertEquals(5, dp.getResources().size());
    }
    
    @Test
    public void testGetExistingResource() throws Exception {
        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(true);
        Resource resource = dp.getResource("third-resource");
        Assertions.assertNotNull(resource);
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
        Assertions.assertNotNull(resource);
        List<Object[]>data = resource.getData(false, false, false, false);
        Assertions.assertEquals( 6, data.size());
        Assertions.assertEquals("libreville", data.get(0)[0]);
        Assertions.assertEquals("0.41,9.29", data.get(0)[1]);
        Assertions.assertEquals( "dakar", data.get(1)[0]);
        Assertions.assertEquals("14.71,-17.53", data.get(1)[1]);
        Assertions.assertEquals("ouagadougou", data.get(2)[0]);
        Assertions.assertEquals("12.35,-1.67", data.get(2)[1]);
        Assertions.assertEquals("barranquilla", data.get(3)[0]);
        Assertions.assertEquals("10.98,-74.88", data.get(3)[1]);
        Assertions.assertEquals("cuidad de guatemala", data.get(5)[0]);
        Assertions.assertEquals("14.62,-90.56", data.get(5)[1]);

    }

    @Test
    public void testReadTabseparatedResourceAndDialect() throws Exception {
        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(
                "/fixtures/tab_separated_datapackage_with_dialect.json", true);
        Resource resource = dp.getResource("first-resource");
        Assertions.assertNotNull(resource);
        List<Object[]>data = resource.getData(false, false, false, false);
        Assertions.assertEquals( 6, data.size());
        Assertions.assertEquals("libreville", data.get(0)[0]);
        Assertions.assertEquals("0.41,9.29", data.get(0)[1]);
        Assertions.assertEquals( "dakar", data.get(1)[0]);
        Assertions.assertEquals("14.71,-17.53", data.get(1)[1]);
        Assertions.assertEquals("ouagadougou", data.get(2)[0]);
        Assertions.assertEquals("12.35,-1.67", data.get(2)[1]);
        Assertions.assertEquals("barranquilla", data.get(3)[0]);
        Assertions.assertEquals("10.98,-74.88", data.get(3)[1]);
        Assertions.assertEquals("cuidad de guatemala", data.get(5)[0]);
        Assertions.assertEquals("14.62,-90.56", data.get(5)[1]);
    }


    @Test
    public void testGetNonExistingResource() throws Exception {
        // Create simple multi DataPackage from Json String
        Package dp = this.getDataPackageFromFilePath(true);
        Resource resource = dp.getResource("non-existing-resource");
        Assertions.assertNull(resource);
    }
    
    @Test
    public void testRemoveResource() throws Exception {
        Package dp = this.getDataPackageFromFilePath(true);
        
        Assertions.assertEquals(5, dp.getResources().size());
        
        dp.removeResource("second-resource");
        Assertions.assertEquals(4, dp.getResources().size());
        
        dp.removeResource("third-resource");
        Assertions.assertEquals(3, dp.getResources().size());
        
        dp.removeResource("third-resource");
        Assertions.assertEquals(3, dp.getResources().size());
    }
    
    @Test
    public void testAddValidResource() throws Exception{
        String pathName = "/fixtures/multi_data_datapackage.json";
        Package dp = this.getDataPackageFromFilePath(pathName,true);
        Path sourceFileAbsPath = Paths.get(PackageTest.class.getResource(pathName).toURI());
        String basePath = sourceFileAbsPath.getParent().toString();
        Assertions.assertEquals(5, dp.getResources().size());

        List<File> files = new ArrayList<>();
        for (String s : Arrays.asList("cities.csv", "cities2.csv")) {
            files.add(new File(s));
        }
        Resource resource = Resource.build("new-resource", files, basePath, TableDataSource.getDefaultEncoding());
        Assertions.assertTrue(resource instanceof FilebasedResource);
        dp.addResource(resource);
        Assertions.assertEquals(6, dp.getResources().size());
        
        Resource gotResource = dp.getResource("new-resource");
        Assertions.assertNotNull(gotResource);
    }

    @Test
    @DisplayName("Test getting resource data from a non-tabular datapackage, file based")
    public void testNonTabularPackage() throws Exception{
        String pathName = "/fixtures/datapackages/non-tabular";
        Path resourcePath = TestUtil.getResourcePath(pathName);
        Package dp = new Package(resourcePath, true);

        Resource<?,?> resource = dp.getResource("logo-svg");
        Assertions.assertTrue(resource instanceof FilebasedResource);
        byte[] rawData = (byte[])resource.getRawData();
        String s = new String (rawData).replaceAll("[\n\r]+", "\n");

        byte[] testData = TestUtil.getResourceContent("/fixtures/files/frictionless-color-full-logo.svg");
        String t = new String (testData).replaceAll("[\n\r]+", "\n");
        Assertions.assertEquals(t, s);
    }
/*
    @Test
    @DisplayName("Test getting resource data from a non-tabular datapackage, ZIP based")
    public void testNonTabularPackageFromZip() throws Exception{
        String pathName = "/fixtures/zip/non-tabular.zip";
        Path resourcePath = TestUtil.getResourcePath(pathName);
        Package dp = new Package(resourcePath, true);

        Resource<?,?> resource = dp.getResource("logo-svg");
        Assertions.assertTrue(resource instanceof FilebasedResource);
        byte[] rawData = (byte[])resource.getRawData();
        String s = new String (rawData).replaceAll("[\n\r]+", "\n");

        byte[] testData = TestUtil.getResourceContent("/fixtures/files/frictionless-color-full-logo.svg");
        String t = new String (testData).replaceAll("[\n\r]+", "\n");
        Assertions.assertEquals(t, s);
    }*/


    @Test
    @DisplayName("Test getting resource data from a non-tabular datapackage, URL based")
    public void testNonTabularPackageUrl() throws Exception{
        URL input = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master" +
                "/src/test/resources/fixtures/datapackages/non-tabular/datapackage.json");

        Package dp = new Package(input, true);

        Resource<?,?> resource = dp.getResource("logo-svg");
        Assertions.assertTrue(resource instanceof URLbasedResource);
        byte[] rawData = (byte[])resource.getRawData();
        String s = new String (rawData).replaceAll("[\n\r]+", "\n");

        byte[] testData = TestUtil.getResourceContent("/fixtures/files/frictionless-color-full-logo.svg");
        String t = new String (testData).replaceAll("[\n\r]+", "\n");
        Assertions.assertEquals(t, s);
    }

    @Test
    @DisplayName("Test setting the 'profile' property")
    public void testSetProfile() throws Exception {
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        String fName = "/fixtures/datapackages/employees/datapackage.json";
        Path resourcePath = TestUtil.getResourcePath(fName);
        Package dp = new Package(resourcePath, true);

        Assertions.assertEquals(Profile.PROFILE_TABULAR_DATA_PACKAGE, dp.getProfile());

        dp.setProfile(PROFILE_DATA_PACKAGE_DEFAULT);
        Assertions.assertEquals(Profile.PROFILE_DATA_PACKAGE_DEFAULT, dp.getProfile());

        File outFile = new File (tempDirPath.toFile(), "datapackage.json");
        dp.writeJson(outFile);
        String content = String.join("\n", Files.readAllLines(outFile.toPath()));
        JsonNode jsonNode = JsonUtil.getInstance().readValue(content);
        String profile = jsonNode.get("profile").asText();
        Assertions.assertEquals(Profile.PROFILE_DATA_PACKAGE_DEFAULT, profile);
        Assertions.assertEquals(Profile.PROFILE_DATA_PACKAGE_DEFAULT, dp.getProfile());
    }

    @Test
    @DisplayName("Test setting invalid 'profile' property, must throw")
    public void testSetInvalidProfile() throws Exception {
        String fName = "/fixtures/datapackages/employees/datapackage.json";
        Path resourcePath = TestUtil.getResourcePath(fName);
        Package dp = new Package(resourcePath, true);

        Assertions.assertThrows(DataPackageValidationException.class,
                () -> dp.setProfile(PROFILE_DATA_RESOURCE_DEFAULT));
        Assertions.assertThrows(DataPackageValidationException.class,
                () -> dp.setProfile(PROFILE_TABULAR_DATA_RESOURCE));
    }

    @Test
    public void testCreateInvalidJSONResource() throws Exception {
        Package dp = this.getDataPackageFromFilePath(true);
        DataPackageException dpe = assertThrows(DataPackageException.class,
                () -> {Resource res = new JSONDataResource(null, testResources.toString());
                    dp.addResource(res);});
        Assertions.assertEquals("Invalid Resource, it does not have a name property.", dpe.getMessage());
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
        Resource resource = Resource.build("third-resource", files, basePath, TableDataSource.getDefaultEncoding());
        Assertions.assertTrue(resource instanceof FilebasedResource);

        DataPackageException dpe = assertThrows(DataPackageException.class, () -> dp.addResource(resource));
        Assertions.assertEquals("A resource with the same name already exists.", dpe.getMessage());
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
        Resource resource = Resource.build("third-resource", files, basePath, TableDataSource.getDefaultEncoding());
        Assertions.assertTrue(resource instanceof FilebasedResource);
        dp.addResource((FilebasedResource)resource);
        
        Assertions.assertEquals(1, dp.getErrors().size());
        Assertions.assertEquals("A resource with the same name already exists.", dp.getErrors().get(0).getMessage());
    }
    
    
    @Test
    public void testSaveToJsonFile() throws Exception{
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        
        Package savedPackage = this.getDataPackageFromFilePath(true);
        savedPackage.write(tempDirPath.toFile(), false);

        Package readPackage = new Package(tempDirPath.resolve(Package.DATAPACKAGE_FILENAME),false);
        JsonNode readPackageJson = createNode(readPackage.getJson()) ;
        JsonNode savedPackageJson = createNode(savedPackage.getJson()) ;
        Assertions.assertTrue(readPackageJson.equals(savedPackageJson));
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
        Assertions.assertEquals(expected, actual);
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
        Assertions.assertEquals(2, data.size());
        Assertions.assertArrayEquals(usdTestData, data.get(0));
        Assertions.assertArrayEquals(gbpTestData, data.get(1));
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
        Assertions.assertFalse(createdFile.exists());
    }

    // Archive file name doesn't end with ".zip"
    @Test
    @DisplayName("Read package from a ZIP file with different suffix")
    public void testReadFromZipFileWithDifferentSuffix() throws Exception{
        String[] usdTestData = new String[]{"USD", "US Dollar", "$"};
        String[] gbpTestData = new String[]{"GBP", "Pound Sterling", "£"};
        String sourceFileAbsPath = ResourceTest.class.getResource("/fixtures/zip/countries-and-currencies.zap").getPath();

        Package dp = new Package(new File(sourceFileAbsPath).toPath(), true);
        Resource r = dp.getResource("currencies");

        List<Object[]> data = r.getData(false, false, false, false);
        Assertions.assertEquals(2, data.size());
        Assertions.assertArrayEquals(usdTestData, data.get(0));
        Assertions.assertArrayEquals(gbpTestData, data.get(1));
    }

    @Test
    @DisplayName("Datapackage with invalid name for descriptor (ie. not 'datapackage.java', must throw")
    public void testReadFromZipFileWithInvalidDatapackageFilenameInside() throws Exception{
        String sourceFileAbsPath = PackageTest.class.getResource("/fixtures/zip/invalid_filename_datapackage.zip").getPath();

        DataPackageException dpe = assertThrows(DataPackageException.class,
                () -> new Package(new File(sourceFileAbsPath).toPath(), false));
        Assertions.assertEquals("The zip file does not contain the expected file: datapackage.json", dpe.getMessage());
    }
    
    @Test
    @DisplayName("Read package from a ZIP with invalid descriptor, must throw")
    public void testReadFromZipFileWithInvalidDatapackageDescriptorAndStrictValidation() throws Exception{
        Path sourceFileAbsPath = Paths
                .get(PackageTest.class.getResource("/fixtures/zip/invalid_datapackage.zip").toURI());

        assertThrows(DataPackageException.class,
                () -> new Package(sourceFileAbsPath.toFile().toPath(), true));
    }
    
    @Test
    @DisplayName("Read package from a non-existing path, must throw")
    public void testReadFromInvalidZipFilePath() throws Exception{
        File invalidFile = new File ("/invalid/path/does/not/exist/datapackage.zip");
        assertThrows(DataPackageFileOrUrlNotFoundException.class,
                () -> new Package(invalidFile.toPath(), false));
    }

    @Test
    @DisplayName("Write datapackage with an image to a folder")
    public void testWriteImageToFolderPackage() throws Exception{
        File dataDirectory = TestUtil.getTestDataDirectory();
        Package pkg = new Package(new File( getBasePath().toFile(), "datapackages/employees/datapackage.json").toPath(), false);
        File imgFile = new File (dataDirectory, "fixtures/files/frictionless-color-full-logo.svg");
        byte [] fileData = Files.readAllBytes(imgFile.toPath());
        Path tempDirPath = Files.createTempDirectory("datapackage-");

        pkg.setImage("logo/ file.svg", fileData);
        File dir = new File (tempDirPath.toFile(), "with-image");
        Path dirPath = Files.createDirectory(dir.toPath(), new FileAttribute[] {});
        pkg.write(dirPath.toFile(), false);
        System.out.println(tempDirPath);
        File descriptor = new File (dir, "datapackage.json");
        String json = String.join("\n", Files.readAllLines(descriptor.toPath()));
        Assertions.assertFalse(json.contains("\"image\""));
    }

    @Test
    @DisplayName("Write datapackage with an image to a ZIP file")
    public void testWriteImageToZipPackage() throws Exception{
        File dataDirectory = TestUtil.getTestDataDirectory();
        File imgFile = new File (dataDirectory, "fixtures/files/frictionless-color-full-logo.svg");
        byte [] fileData = Files.readAllBytes(imgFile.toPath());
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        Path resourcePath = TestUtil.getResourcePath("/fixtures/zip/countries-and-currencies.zip");
        File createdFile = new File(tempDirPath.toFile(), "test_save_datapackage.zip");
        Files.copy(resourcePath, createdFile.toPath());

        Package dp = new Package(createdFile.toPath(), true);
        dp.setImage("logo/ file.svg", fileData);
        dp.write(new File(tempDirPath.toFile(), "with-image.zip"), true);
        System.out.println(tempDirPath);
    }


    @Test
    @DisplayName("Write datapackage using a Consumer function to fingerprint files")
    public void testWriteWithConsumer() throws Exception{
        File refDescriptor = new File(getBasePath().toFile(), "datapackages/employees/datapackage.json");
        Package pkg = new Package(refDescriptor.toPath(), false);
        Path tempDirPath = Files.createTempDirectory("datapackage-");

        File dir = new File (tempDirPath.toFile(), "test-package");
        Path dirPath = Files.createDirectory(dir.toPath(), new FileAttribute[] {});
        pkg.write(dirPath.toFile(), PackageTest::fingerprintFiles, false);
        System.out.println(tempDirPath);
        File fingerprints = new File (dir, "fingerprints.txt");
        String content = String.join("\n", Files.readAllLines(fingerprints.toPath()));
        String refContent =
                "datapackage.json\te3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n" +
                        "schema.json\te3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        Assertions.assertEquals(refContent, content);
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
            
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], location);
            
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
            
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], location);
            
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
        Assertions.assertEquals(expectedSchema, resource.getSchema());

        // Get JSON Object
        JsonNode expectedSchemaJson = createNode(expectedSchema.getJson());
        JsonNode testSchemaJson = createNode(resource.getSchema().getJson());
        // Compare JSON objects
        Assertions.assertEquals(expectedSchemaJson, testSchemaJson, "Schemas don't match");
    }
    
    @Test
    public void testResourceSchemaDereferencingForRemoteDataFileAndLocalSchemaFile() throws Exception {
        Package pkg = this.getDataPackageFromFilePath(true);
        Resource resource = pkg.getResource("fourth-resource");

        // Get string content version of the schema file.
        String schemaJsonString =getFileContents("/fixtures/schema/population_schema.json");

        Schema expectedSchema = Schema.fromJson(schemaJsonString, true);
        Assertions.assertEquals(expectedSchema, resource.getSchema());

        // Get JSON Object
        JsonNode expectedSchemaJson = createNode(expectedSchema.getJson());
        JsonNode testSchemaJson = createNode(resource.getSchema().getJson());
        // Compare JSON objects
        Assertions.assertEquals(expectedSchemaJson, testSchemaJson, "Schemas don't match");
    }

    @Test
    public void testAddPackageProperty() throws Exception{
        Object[] entries = new Object[]{"K", 3.2, 2};
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("time", "s");
        props.put("length", 3.2);
        props.put("count", 7);

        Path pkgFile =  TestUtil.getResourcePath("/fixtures/datapackages/employees/datapackage.json");
        Package p = new Package(pkgFile, true);

        p.setProperty("mass unit", "kg");
        p.setProperty("mass flow", 3.2);
        p.setProperty("number of parcels", 5);
        p.setProperty("entries", entries);
        p.setProperty("props", props);
        p.setProperty("null", null);
        Assertions.assertEquals("kg", p.getProperty("mass unit"), "JSON doesn't match");
        Assertions.assertEquals(new BigDecimal("3.2"), p.getProperty("mass flow"), "JSON doesn't match");
        Assertions.assertEquals(new BigInteger("5"), p.getProperty("number of parcels"), "JSON doesn't match");
        Assertions.assertEquals(Arrays.asList(entries), p.getProperty("entries"), "JSON doesn't match");
        Assertions.assertEquals(props, p.getProperty("props"), "JSON doesn't match");
        Assertions.assertNull(p.getProperty("null"));
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
        Assertions.assertEquals("kg", p.getProperty("mass unit"), "JSON doesn't match");
        Assertions.assertEquals(new BigDecimal("3.2"), p.getProperty("mass flow"), "JSON doesn't match");
        Assertions.assertEquals( new BigInteger("5"), p.getProperty("number of parcels"), "JSON doesn't match");
        Assertions.assertEquals(Arrays.asList(entries), p.getProperty("entries"), "JSON doesn't match");
        Assertions.assertEquals(props, p.getProperty("props"), "JSON doesn't match");
        Assertions.assertNull(p.getProperty("null"), "JSON doesn't match");
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
        Assertions.assertEquals(Dialect.fromJson(dialectJsonString), resource.getDialect());
    }

    @Test
    public void testAdditionalProperties() throws Exception {
        String fName = "/fixtures/datapackages/additional-properties/datapackage.json";
        String sourceFileAbsPath = PackageTest.class.getResource(fName).getPath();
        Package dp = new Package(new File(sourceFileAbsPath).toPath(), true);

        Object creator = dp.getProperty("creator");
        Assertions.assertNotNull(creator);
        Assertions.assertEquals("Horst", creator);

        Object testprop = dp.getProperty("testprop");
        Assertions.assertNotNull(testprop);
        Assertions.assertTrue(testprop instanceof Map);

        Object testarray = dp.getProperty("testarray");
        Assertions.assertNotNull(testarray);
        Assertions.assertTrue(testarray instanceof ArrayList);

        Object resObj = dp.getProperty("something");
        Assertions.assertNull(resObj);
    }

    @Test
    public void testBeanResource1() throws Exception {
        Package pkg = new Package(new File(getBasePath().toFile(), "datapackages/bean-iterator/datapackage.json").toPath(), true);

        Resource resource = pkg.getResource("employee-data");
        final List<EmployeeBean> employees = resource.getData(EmployeeBean.class);
        Assertions.assertEquals(3, employees.size());
        EmployeeBean frank = employees.get(1);
        Assertions.assertEquals("Frank McKrank", frank.getName());
        Assertions.assertEquals("1992-02-14", new DateField("date").formatValueAsString(frank.getDateOfBirth(), null, null));
        Assertions.assertFalse(frank.getAdmin());
        Assertions.assertEquals("(90.0, 45.0, NaN)", frank.getAddressCoordinates().toString());
        Assertions.assertEquals("PT15M", frank.getContractLength().toString());
        Map info = frank.getInfo();
        Assertions.assertEquals(45, info.get("pin"));
        Assertions.assertEquals(83.23, info.get("rate"));
        Assertions.assertEquals(90, info.get("ssn"));
    }

    private static void fingerprintFiles(Path path) {
        System.out.println(path);
        List<String> fingerprints = new ArrayList<>();
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");

            for (File f : path.toFile().listFiles()) {
                if (f.isFile()) {
                    /*try (DigestInputStream dis = new DigestInputStream(Files.newInputStream(f.toPath()), md)) {
                        while (true) {
                            if (dis.read() == -1) break;
                        }
                        md = dis.getMessageDigest();
                    }*/
                    String content = String.join("\n", Files.readAllLines(f.toPath()));
                    content = content.replaceAll("[\\n\\r]+", "\n");
                    md.digest(content.getBytes());

                    StringBuilder result = new StringBuilder();
                    for (byte b : md.digest()) {
                        result.append(String.format("%02x", b));
                    }
                    fingerprints.add(f.getName() + "\t" + result);
                    md.reset();
                }
            }

            File outFile = new File(path.toFile(), "fingerprints.txt");
            try (FileWriter wr = new FileWriter(outFile)) {
                wr.write(String.join("\n", fingerprints));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
            return new String(TestUtil.getResourceContent(fileName));
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
