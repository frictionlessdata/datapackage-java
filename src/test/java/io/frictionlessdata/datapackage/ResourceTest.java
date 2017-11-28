package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * 
 */
public class ResourceTest {
    
    @Test
    public void testIterateDataFromUrlPath() throws IOException, DataPackageException{
       
        String urlString = "https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/data/population.csv";
        Resource resource = new Resource("population", urlString);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();
        
        // Get iterator.
        Iterator<CSVRecord> iter = resource.iter();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            CSVRecord record = iter.next();
            String city = record.get(0);
            String year = record.get(1);
            String population = record.get(2);
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
            expectedDataIndex++;
        } 
    }
            
    
    @Test
    public void testIterateDataFromFilePath() throws IOException, DataPackageException, MalformedURLException{

        String filePath = ResourceTest.class.getResource("/fixtures/data/population.csv").getPath();
        Resource resource = new Resource("population", filePath);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();
        
        // Get iterator.
        Iterator<CSVRecord> iter = resource.iter();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            CSVRecord record = iter.next();
            String city = record.get(0);
            String year = record.get(1);
            String population = record.get(2);
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
            expectedDataIndex++;
        }    
    }
    
    @Test
    public void testIterateDataFromCsvFormat() throws IOException, DataPackageException{
        String filePath = ResourceTest.class.getResource("/fixtures/data/population.csv").getPath();
        
        String dataString = "city,year,population\nlondon,2017,8780000\nparis,2017,2240000\nrome,2017,2860000";
        Resource resource = new Resource("population", dataString, Resource.FORMAT_CSV);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = this.getExpectedPopulationData();
        
        // Get Iterator.
        Iterator<CSVRecord> iter = resource.iter();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            CSVRecord record = iter.next();
            String city = record.get(0);
            String year = record.get(1);
            String population = record.get(2);
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
            expectedDataIndex++;
        }  
    }
    
    @Test
    public void testIterateDataFromJSONFormat() throws IOException, DataPackageException{
        String filePath = ResourceTest.class.getResource("/fixtures/data/population.csv").getPath();
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
        Iterator<CSVRecord> iter = resource.iter();
        int expectedDataIndex = 0;
        
        // Assert data.
        while(iter.hasNext()){
            CSVRecord record = iter.next();
            String city = record.get(0);
            String year = record.get(1);
            String population = record.get(2);
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
            expectedDataIndex++;
        } 
    }
    
    private List<String[]> getExpectedPopulationData(){
        List<String[]> expectedData  = new ArrayList();
        expectedData.add(new String[]{"city", "year", "population"});
        expectedData.add(new String[]{"london", "2017", "8780000"});
        expectedData.add(new String[]{"paris", "2017", "2240000"});
        expectedData.add(new String[]{"rome", "2017", "2860000"});
        
        return expectedData;
    }

}
