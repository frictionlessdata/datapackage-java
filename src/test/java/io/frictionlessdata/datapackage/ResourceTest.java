package io.frictionlessdata.datapackage;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * 
 */
public class ResourceTest {
    
    @Test
    public void testIterateDataFromUrlPath() throws Exception{
       
        String urlString = "https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/data/population.csv";
        URL dataSource = new URL(urlString);
        Resource resource = new Resource("population", dataSource);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();
        
        // Get iterator.
        Iterator<String[]> iter = resource.iter();
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

        String filePath = ResourceTest.class.getResource("/fixtures/data/population.csv").getPath();
        File file = new File(filePath);
        Resource resource = new Resource("population", file);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();
        
        // Get iterator.
        Iterator<String[]> iter = resource.iter();
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
        
        JSONArray multipartPathJsonArray = new JSONArray("[\"src/test/resources/fixtures/data/cities.csv\", \"src/test/resources/fixtures/data/cities2.csv\", \"src/test/resources/fixtures/data/cities3.csv\"]");
        Resource resource = new Resource("coordinates", multipartPathJsonArray);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        Iterator<String[]> iter = resource.iter();
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
        
        JSONArray multipartPathJsonArray = new JSONArray("[\"https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/data/cities.csv\", \"https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/data/cities2.csv\", \"https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/data/cities3.csv\"]");
        Resource resource = new Resource("coordinates", multipartPathJsonArray);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        Iterator<String[]> iter = resource.iter();
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
    public void testIterateDataFromCsvFormat() throws Exception{
        String dataString = "city,year,population\nlondon,2017,8780000\nparis,2017,2240000\nrome,2017,2860000";
        Resource resource = new Resource("population", dataString, Resource.FORMAT_CSV);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();
        
        // Get Iterator.
        Iterator<String[]> iter = resource.iter();
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
        JSONArray jsonData = new JSONArray("[" +
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
        "]");
        
        Resource resource = new Resource("population", jsonData, Resource.FORMAT_JSON);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();
        
        // Get Iterator.
        Iterator<String[]> iter = resource.iter();
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
    
    private List<String[]> getExpectedPopulationData(){
        List<String[]> expectedData  = new ArrayList();
        //expectedData.add(new String[]{"city", "year", "population"});
        expectedData.add(new String[]{"london", "2017", "8780000"});
        expectedData.add(new String[]{"paris", "2017", "2240000"});
        expectedData.add(new String[]{"rome", "2017", "2860000"});
        
        return expectedData;
    }
}
