package io.frictionlessdata.datapackage.resource;

import java.io.File;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import io.frictionlessdata.datapackage.Package;
import io.frictionlessdata.datapackage.PackageTest;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.schema.Schema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 *
 * 
 */
public class ResourceTest {
    static JSONObject resource1 = new JSONObject("{\"name\": \"first-resource\", \"path\": " +
            "[\"data/cities.csv\", \"data/cities2.csv\", \"data/cities3.csv\"]}");
    static JSONObject resource2 = new JSONObject("{\"name\": \"second-resource\", \"path\": " +
            "[\"data/area.csv\", \"data/population.csv\"]}");

    static JSONArray testResources;

    static {
        testResources = new JSONArray();
        testResources.put(resource1);
        testResources.put(resource2);
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();

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
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
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
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
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
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], coords);
            
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
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], coords);
            
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
        
        Iterator<Object[]> iter = resource.objectArrayIterator(false, false, true, false);
        
        // Assert data.
        while(iter.hasNext()){
            Object[] record = iter.next();
            
            Assert.assertEquals(String.class, record[0].getClass());
            Assert.assertEquals(Year.class, record[1].getClass());
            Assert.assertEquals(BigInteger.class, record[2].getClass());
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

            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);

            expectedDataIndex++;
        }
    }


    @Test
    public void testBuildAndIterateDataFromCsvFormat() throws Exception{
        String dataString = getFileContents("/fixtures/resource/valid_csv_resource.json");
        Resource resource = Resource.build(new JSONObject(dataString), getBasePath(), false);

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

            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);

            expectedDataIndex++;
        }
    }

    @Test
    public void testBuildAndIterateDataFromTabseparatedCsvFormat() throws Exception{
        String dataString = getFileContents("/fixtures/resource/valid_csv_resource_tabseparated.json");
        Resource resource = Resource.build(new JSONObject(dataString), getBasePath(), false);

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

            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);

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

        JSONDataResource<String[]> resource = new JSONDataResource<>("population", jsonData);

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
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
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

        JSONDataResource<String[]> resource = new JSONDataResource<>("population", jsonData);

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

            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);

            expectedDataIndex++;
        }
    }

    // Test is invalid as the order of properties in a JSON object is not guaranteed (see spec)
    @Test
    public void testBuildAndIterateDataFromJSONFormat() throws Exception{
        String dataString = getFileContents("/fixtures/resource/valid_json_array_resource.json");
        Resource resource = Resource.build(new JSONObject(dataString), getBasePath(), false);

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

            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);

            expectedDataIndex++;
        }
    }


    /*
    FIXME: since strongly typed, those don't work anymore
    @Test
    public void testCreatingJSONResourceWithInvalidFormatNullValue() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/" +
                "datapackage-java/master/src/test/resources/fixtures/multi_data_datapackage.json");
        Package dp = new Package(url, true);

        // format property is null but data is not null.
        Resource resource = new JSONDataResource("resource-name", testResources, (String)null);

        exception.expectMessage("Invalid Resource. The data and format properties cannot be null.");
        dp.addResource(resource);
    }

    @Test
    public void testCreatingResourceWithInvalidFormatDataValue() throws Exception {
        URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/multi_data_datapackage.json");
        Package dp = new Package(url, true);

        // data property is null but format is not null.
        Resource resource = new JSONDataResource("resource-name", (String)null, "csv");

        exception.expectMessage("Invalid Resource. The path property or the data and format properties cannot be null.");
        dp.addResource(resource);
    }

     */
    @Test
    public void testRead() throws Exception{
        Resource resource = buildResource("/fixtures/data/population.csv");
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Assert
        Assert.assertEquals(3, resource.getData(false, false, false, false).size());
    }


    @Test
    public void testReadFromZipFile() throws Exception{
        String sourceFileAbsPath = ResourceTest.class.getResource("/fixtures/zip/countries-and-currencies.zip").getPath();

        Package dp = new Package(new File(sourceFileAbsPath).toPath(), true);
        Resource r = dp.getResource("currencies");

        List<Object[]> data = r.getData(false, false, false, false);
    }
    
    @Test
    public void testHeadings() throws Exception{
        Resource resource = buildResource("/fixtures/data/population.csv");
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Assert
        Assert.assertEquals("city", resource.getHeaders()[0]);
        Assert.assertEquals("year", resource.getHeaders()[1]);
        Assert.assertEquals("population", resource.getHeaders()[2]);
    }


    @Test
    public void readCreateInvalidResourceContainingAbsolutePaths() throws Exception{
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        URI sourceFileAbsPathURI1 = PackageTest.class.getResource("/fixtures/data/cities.csv").toURI();
        URI sourceFileAbsPathURI2 = PackageTest.class.getResource("/fixtures/data/cities2.csv").toURI();
        File sourceFileAbsPathU1 = Paths.get(sourceFileAbsPathURI1).toAbsolutePath().toFile();
        File sourceFileAbsPathU2 = Paths.get(sourceFileAbsPathURI2).toAbsolutePath().toFile();
        ArrayList<File> files = new ArrayList<>();
        files.add(sourceFileAbsPathU1);
        files.add(sourceFileAbsPathU2);

        exception.expect(DataPackageException.class);
        FilebasedResource r = new FilebasedResource("resource-one", files, getBasePath());
        Package pkg = new Package("test", tempDirPath.resolve("datapackage.json"), true);
        pkg.addResource(r);
    }

    private static Resource buildResource(String relativeInPath) throws URISyntaxException {
        URL sourceFileUrl = ResourceTest.class.getResource(relativeInPath);
        Path path = Paths.get(sourceFileUrl.toURI());
        Path parent = path.getParent();
        Path relativePath = parent.relativize(path);

        List<File> files = new ArrayList<>();
        files.add(relativePath.toFile());
        return new FilebasedResource("population", files, parent.toFile());
    }

    private static File getBasePath() throws URISyntaxException {
        URL sourceFileUrl = ResourceTest.class.getResource("/fixtures/data");
        Path path = Paths.get(sourceFileUrl.toURI());
        return path.getParent().toFile();
    }

    private static String getFileContents(String fileName) {
        try {
            // Create file-URL of source file:
            URL sourceFileUrl = ResourceTest.class.getResource(fileName);
            // Get path of URL
            Path path = Paths.get(sourceFileUrl.toURI());
            return new String(Files.readAllBytes(path));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private List<String[]> getExpectedPopulationData(){
        List<String[]> expectedData  = new ArrayList();
        //expectedData.add(new String[]{"city", "year", "population"});
        expectedData.add(new String[]{"london", "2017", "8780000"});
        expectedData.add(new String[]{"paris", "2017", "2240000"});
        expectedData.add(new String[]{"rome", "2017", "2860000"});
        
        return expectedData;
    }

    private List<String[]> getExpectedAlternatePopulationData(){
        List<String[]> expectedData  = new ArrayList();
        expectedData.add(new String[]{"2017", "london", "8780000"});
        expectedData.add(new String[]{"2017", "paris", "2240000"});
        expectedData.add(new String[]{"2017", "rome", "2860000"});

        return expectedData;
    }
}
