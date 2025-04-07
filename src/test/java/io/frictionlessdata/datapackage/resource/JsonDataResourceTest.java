package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.frictionlessdata.datapackage.Package;
import io.frictionlessdata.datapackage.PackageTest;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Year;
import java.util.*;

import static io.frictionlessdata.datapackage.Profile.*;
import static io.frictionlessdata.datapackage.TestUtil.getTestDataDirectory;

/**
 *
 * 
 */
public class JsonDataResourceTest {
    static ObjectNode resource1 = (ObjectNode) JsonUtil.getInstance().createNode("{\"name\": \"first-resource\", \"path\": " +
            "[\"data/cities.csv\", \"data/cities2.csv\", \"data/cities3.csv\"]}");
    static ObjectNode resource2 = (ObjectNode) JsonUtil.getInstance().createNode("{\"name\": \"second-resource\", \"path\": " +
            "[\"data/area.csv\", \"data/population.csv\"]}");

    static ArrayNode testResources;

    static {
        testResources = JsonUtil.getInstance().createArrayNode();
        testResources.add(resource1);
        testResources.add(resource2);
    }

    @Test
    public void testIterateDataFromUrlPath() throws Exception{
       
        String urlString = "https://raw.githubusercontent.com/frictionlessdata/datapackage-java" +
                "/master/src/test/resources/fixtures/data/population.csv";
        List<URL> dataSource = Arrays.asList(new URL(urlString));
        Resource resource = new URLbasedResource("population", dataSource);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();
        
        // Get objectArrayIterator.
        Iterator<String[]> iter = resource.objectArrayIterator();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String year = record[1];
            String population = record[2];
            
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
            expectedDataIndex++;
        } 
    }
            
    @Test
    public void testIterateDataFromFilePath() throws Exception{
        Resource resource = buildResource("/fixtures/data/population.csv");
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();
        
        // Get objectArrayIterator.
        Iterator<String[]> iter = resource.objectArrayIterator();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String year = record[1];
            String population = record[2];
            
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
            expectedDataIndex++;
        }    
    }
    
    @Test
    public void testIterateDataFromMultipartFilePath() throws Exception{
        List<String[]> expectedData  = new ArrayList();
        expectedData.add(new String[]{"libreville", "0.41,9.29"});
        expectedData.add(new String[]{"dakar", "14.71,-17.53"});
        expectedData.add(new String[]{"ouagadougou", "12.35,-1.67"});
        expectedData.add(new String[]{"barranquilla", "10.98,-74.88"});
        expectedData.add(new String[]{"rio de janeiro", "-22.91,-43.72"});
        expectedData.add(new String[]{"cuidad de guatemala", "14.62,-90.56"});
        expectedData.add(new String[]{"london", "51.50,-0.11"});
        expectedData.add(new String[]{"paris", "48.85,2.30"});
        expectedData.add(new String[]{"rome", "41.89,12.51"});
        
        String[] paths = new String[]{
                "data/cities.csv",
                "data/cities2.csv",
                "data/cities3.csv"};
        List<File> files = new ArrayList<>();
        for (String file : paths) {
            files.add(new File (file));
        }
        Resource resource = new FilebasedResource("coordinates", files, getBasePath());

        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        Iterator<String[]> iter = resource.objectArrayIterator();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String coords = record[1];
            
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], coords);
            
            expectedDataIndex++;
        }
    }
    
    @Test
    public void testIterateDataFromMultipartURLPath() throws Exception{
        List<String[]> expectedData  = new ArrayList();
        expectedData.add(new String[]{"libreville", "0.41,9.29"});
        expectedData.add(new String[]{"dakar", "14.71,-17.53"});
        expectedData.add(new String[]{"ouagadougou", "12.35,-1.67"});
        expectedData.add(new String[]{"barranquilla", "10.98,-74.88"});
        expectedData.add(new String[]{"rio de janeiro", "-22.91,-43.72"});
        expectedData.add(new String[]{"cuidad de guatemala", "14.62,-90.56"});
        expectedData.add(new String[]{"london", "51.50,-0.11"});
        expectedData.add(new String[]{"paris", "48.85,2.30"});
        expectedData.add(new String[]{"rome", "41.89,12.51"});

        String[] paths = new String[]{"https://raw.githubusercontent.com" +
                "/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/data/cities.csv",
                "https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test" +
                "/resources/fixtures/data/cities2.csv",
                "https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src" +
                "/test/resources/fixtures/data/cities3.csv"};
        List<URL> urls = new ArrayList<>();
        for (String file : paths) {
            urls.add(new URL (file));
        }
        Resource resource = new URLbasedResource("coordinates", urls);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        Iterator<String[]> iter = resource.objectArrayIterator();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String coords = record[1];
            
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], coords);
            
            expectedDataIndex++;
        }
    }
    
    @Test
    public void testIterateDataWithCast() throws Exception{
        // Get string content version of the schema file.
        String schemaJsonString = getFileContents("/fixtures/schema/population_schema.json");

        Resource resource = buildResource("/fixtures/data/population.csv");

        //set schema
        resource.setSchema(Schema.fromJson(schemaJsonString, true));
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        Iterator<Object[]> iter = resource.objectArrayIterator(false,  false);
        
        // Assert data.
        while(iter.hasNext()){
            Object[] record = iter.next();
            
            Assertions.assertEquals(String.class, record[0].getClass());
            Assertions.assertEquals(Year.class, record[1].getClass());
            Assertions.assertEquals(BigInteger.class, record[2].getClass());
        }
    }

    
    @Test
    public void testIterateDataFromCsvFormat() throws Exception{
        String dataString = "city,year,population\nlondon,2017,8780000\nparis,2017,2240000\nrome,2017,2860000";
        Resource resource = new CSVDataResource("population", dataString);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();

        // Get Iterator.
        Iterator<String[]> iter = resource.objectArrayIterator();
        int expectedDataIndex = 0;

        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String year = record[1];
            String population = record[2];

            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[2], population);

            expectedDataIndex++;
        }
    }


    @Test
    public void testBuildAndIterateDataFromCsvFormat() throws Exception{
        String dataString = getFileContents("/fixtures/resource/valid_csv_resource.json");
        Resource resource = Resource.build((ObjectNode) JsonUtil.getInstance().createNode(dataString), getBasePath(), false);

        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();

        // Get Iterator.
        Iterator<String[]> iter = resource.objectArrayIterator();
        int expectedDataIndex = 0;

        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String year = record[1];
            String population = record[2];

            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[2], population);

            expectedDataIndex++;
        }
    }

    @Test
    public void testBuildAndIterateDataFromTabseparatedCsvFormat() throws Exception{
        String dataString = getFileContents("/fixtures/resource/valid_csv_resource_tabseparated.json");
        Resource resource = Resource.build((ObjectNode) JsonUtil.getInstance().createNode(dataString), getBasePath(), false);

        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();

        // Get Iterator.
        Iterator<String[]> iter = resource.objectArrayIterator();
        int expectedDataIndex = 0;

        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String year = record[1];
            String population = record[2];

            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[2], population);

            expectedDataIndex++;
        }
    }

    @Test
    public void testIterateDataFromJSONFormat() throws Exception{
        String jsonData = "[" +
            "{" +
              "\"city\": \"london\"," +
              "\"year\": 2017," +
              "\"population\": 8780000" +
            "}," +
            "{" +
              "\"city\": \"paris\"," +
              "\"year\": 2017," +
              "\"population\": 2240000" +
            "}," +
            "{" +
              "\"city\": \"rome\"," +
              "\"year\": 2017," +
              "\"population\": 2860000" +
            "}" +
        "]";

        JSONDataResource resource = new JSONDataResource("population", jsonData);

        //set a schema to guarantee the ordering of properties
        Schema schema = Schema.fromJson(new File(getBasePath(), "/schema/population_schema.json"), true);
        resource.setSchema(schema);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();
        
        // Get Iterator.
        Iterator<String[]> iter = resource.stringArrayIterator();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String year = record[1];
            String population = record[2];
            
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
            expectedDataIndex++;
        } 
    }

    @Test
    public void testIterateDataFromJSONFormatAlternateSchema() throws Exception{
        String jsonData = "[" +
                "{" +
                "\"city\": \"london\"," +
                "\"year\": 2017," +
                "\"population\": 8780000" +
                "}," +
                "{" +
                "\"city\": \"paris\"," +
                "\"year\": 2017," +
                "\"population\": 2240000" +
                "}," +
                "{" +
                "\"city\": \"rome\"," +
                "\"year\": 2017," +
                "\"population\": 2860000" +
                "}" +
                "]";

        JSONDataResource resource = new JSONDataResource("population", jsonData);

        //set a schema to guarantee the ordering of properties
        Schema schema = Schema.fromJson(new File(getBasePath(), "/schema/population_schema_alternate.json"), true);
        resource.setSchema(schema);

        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);

        // Expected data.
        List<String[]> expectedData = this.getExpectedAlternatePopulationData();

        // Get Iterator.
        Iterator<String[]> iter = resource.stringArrayIterator();
        int expectedDataIndex = 0;

        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String year = record[1];
            String population = record[2];

            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[2], population);

            expectedDataIndex++;
        }
    }

    // Test is invalid as the order of properties in a JSON object is not guaranteed (see spec)
    @Test
    public void testBuildAndIterateDataFromJSONFormat() throws Exception{
        String dataString = getFileContents("/fixtures/resource/valid_json_array_resource.json");
        Resource resource = Resource.build((ObjectNode) JsonUtil.getInstance().createNode(dataString), getBasePath(), false);

        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();

        // Get Iterator.
        Iterator<String[]> iter = resource.objectArrayIterator();
        int expectedDataIndex = 0;

        // Assert data.
        while(iter.hasNext()){
            String[] record = iter.next();
            String city = record[0];
            String year = record[1];
            String population = record[2];

            Assertions.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assertions.assertEquals(expectedData.get(expectedDataIndex)[2], population);

            expectedDataIndex++;
        }
    }

    @Test
    public void testRead() throws Exception{
        Resource resource = buildResource("/fixtures/data/population.csv");
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Assert
        Assertions.assertEquals(3, resource.getData(false, false, false, false).size());
    }

    @Test
    public void testReadFromZipFile() throws Exception{
        String sourceFileAbsPath = JsonDataResourceTest.class.getResource("/fixtures/zip/countries-and-currencies.zip").getPath();

        Package dp = new Package(new File(sourceFileAbsPath).toPath(), true);
        Resource r = dp.getResource("currencies");

        List<Object[]> data = r.getData(false, false, false, false);
        Assertions.assertEquals(2, data.size());
        Object[] row1 = data.get(0);
        Assertions.assertEquals("USD", row1[0]);
        Assertions.assertEquals("US Dollar", row1[1]);
        Assertions.assertEquals("$", row1[2]);

        Object[] row2 = data.get(1);
        Assertions.assertEquals("GBP", row2[0]);
        Assertions.assertEquals("Pound Sterling", row2[1]);
        Assertions.assertEquals("£", row2[2]);
    }
    
    @Test
    public void testHeadings() throws Exception{
        Resource resource = buildResource("/fixtures/data/population.csv");
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Assert
        Assertions.assertEquals("city", resource.getHeaders()[0]);
        Assertions.assertEquals("year", resource.getHeaders()[1]);
        Assertions.assertEquals("population", resource.getHeaders()[2]);
    }


    @Test
    @DisplayName("Paths in File-based resources must not be absolute")
    /*
        Test to verify https://specs.frictionlessdata.io/data-resource/#data-location :
        POSIX paths (unix-style with / as separator) are supported for referencing local files,
        with the security restraint that they MUST be relative siblings or children of the descriptor.
        Absolute paths (/) and relative parent paths (…/) MUST NOT be used,
        and implementations SHOULD NOT support these path types.
     */
    public void readCreateInvalidResourceContainingAbsolutePaths() throws Exception{
        URI sourceFileAbsPathURI1 = PackageTest.class.getResource("/fixtures/data/cities.csv").toURI();
        URI sourceFileAbsPathURI2 = PackageTest.class.getResource("/fixtures/data/cities2.csv").toURI();
        File sourceFileAbsPathU1 = Paths.get(sourceFileAbsPathURI1).toAbsolutePath().toFile();
        File sourceFileAbsPathU2 = Paths.get(sourceFileAbsPathURI2).toAbsolutePath().toFile();

        ArrayList<File> files = new ArrayList<>();
        files.add(sourceFileAbsPathU1);
        files.add(sourceFileAbsPathU2);

        Exception dpe = Assertions.assertThrows(DataPackageException.class, () -> {
            new FilebasedResource("resource-one", files, getBasePath());
        });
        Assertions.assertEquals("Path entries for file-based Resources cannot be absolute", dpe.getMessage());
    }

    @Test
    @DisplayName("Test reading Resource data rows as Map<String, Object>, ensuring we get values of " +
            "the correct Schema Field type")
    public void testReadMapped1() throws Exception{
        String[][] referenceData = new String[][]{
                {"city","year","population"},
                {"london","2017","8780000"},
                {"paris","2017","2240000"},
                {"rome","2017","2860000"}};
        Resource<?> resource = buildResource("/fixtures/data/population.csv");
        Schema schema = Schema.fromJson(new File(getTestDataDirectory()
                , "/fixtures/schema/population_schema.json"), true);
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        resource.setSchema(schema);
        List<Map<String, Object>> mappedData = resource.getMappedData(false);
        Assertions.assertEquals(3, mappedData.size());
        String[] headers = referenceData[0];
        //need to omit the table header in the referenceData
        for (int i = 0; i < mappedData.size(); i++) {
            String[] refRow = referenceData[i+1];
            Map<String, Object> testData = mappedData.get(i);
            // ensure row size is correct
            Assertions.assertEquals(refRow.length, testData.size());

            // ensure we get the headers in the right sort order
            ArrayList<String> testDataColKeys = new ArrayList<>(testData.keySet());
            String[] testHeaders = testDataColKeys.toArray(new String[]{});
            Assertions.assertArrayEquals(headers, testHeaders);

            // validate values match and types are as expected
            Assertions.assertEquals(refRow[0], testData.get(testDataColKeys.get(0))); //String value for city name
            Assertions.assertEquals(Year.class, testData.get(testDataColKeys.get(1)).getClass());
            Assertions.assertEquals(refRow[1], ((Year)testData.get(testDataColKeys.get(1))).toString());//Year value for year
            Assertions.assertEquals(BigInteger.class, testData.get(testDataColKeys.get(2)).getClass()); //String value for city name
            Assertions.assertEquals(refRow[2], testData.get(testDataColKeys.get(2)).toString());//BigInteger value for population
        }
    }
    @Test
    @DisplayName("Test setting invalid 'profile' property, must throw")
    public void testSetInvalidProfile() throws Exception {
        Resource<?> resource = buildResource("/fixtures/data/population.csv");

        Assertions.assertThrows(DataPackageValidationException.class,
                () -> resource.setProfile(PROFILE_DATA_PACKAGE_DEFAULT));
        Assertions.assertThrows(DataPackageValidationException.class,
                () -> resource.setProfile(PROFILE_TABULAR_DATA_PACKAGE));
        Assertions.assertDoesNotThrow(() -> resource.setProfile(PROFILE_DATA_RESOURCE_DEFAULT));
        Assertions.assertDoesNotThrow(() -> resource.setProfile(PROFILE_TABULAR_DATA_RESOURCE));
    }

    private static Resource<?> buildResource(String relativeInPath) throws URISyntaxException {
        URL sourceFileUrl = JsonDataResourceTest.class.getResource(relativeInPath);
        Path path = Paths.get(sourceFileUrl.toURI());
        Path parent = path.getParent();
        Path relativePath = parent.relativize(path);

        List<File> files = new ArrayList<>();
        files.add(relativePath.toFile());
        return new FilebasedResource("population", files, parent.toFile());
    }

    private static File getBasePath() throws URISyntaxException {
        URL sourceFileUrl = JsonDataResourceTest.class.getResource("/fixtures/data");
        Path path = Paths.get(sourceFileUrl.toURI());
        return path.getParent().toFile();
    }

    private static String getFileContents(String fileName) {
        try {
            // Create file-URL of source file:
            URL sourceFileUrl = JsonDataResourceTest.class.getResource(fileName);
            // Get path of URL
            Path path = Paths.get(sourceFileUrl.toURI());
            return new String(Files.readAllBytes(path));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private List<String[]> getExpectedPopulationData(){
        List<String[]> expectedData  = new ArrayList<>();
        //expectedData.add(new String[]{"city", "year", "population"});
        expectedData.add(new String[]{"london", "2017", "8780000"});
        expectedData.add(new String[]{"paris", "2017", "2240000"});
        expectedData.add(new String[]{"rome", "2017", "2860000"});
        
        return expectedData;
    }

    private List<String[]> getExpectedAlternatePopulationData(){
        List<String[]> expectedData  = new ArrayList<>();
        expectedData.add(new String[]{"2017", "london", "8780000"});
        expectedData.add(new String[]{"2017", "paris", "2240000"});
        expectedData.add(new String[]{"2017", "rome", "2860000"});

        return expectedData;
    }
}
