package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVRecord;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * 
 */
public class ResourceTest {
    
    @Test
    public void testValidationWithValidProfileUrl() throws IOException, DataPackageException, MalformedURLException{

        String filePath = ResourceTest.class.getResource("/fixtures/data/population.csv").getPath();
        Resource resource = new Resource("population", filePath);
        
        // Set the profile to tabular data resource.
        resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);
        
        // Expected data.
        List<String[]> expectedData = new ArrayList();
        expectedData.add(new String[]{"city", "year", "population"});
        expectedData.add(new String[]{"london", "2017", "8780000"});
        expectedData.add(new String[]{"paris", "2017", "2240000"});
        expectedData.add(new String[]{"rome", "2017", "2860000"});
 
        // Get iterator.
        Iterable<CSVRecord> records = resource.iter();
        int expectedDataIndex = 0;
        
        // Assert data.
        for (CSVRecord record : records) {
            String city = record.get(0);
            String year = record.get(1);
            String population = record.get(2);
            
            Assert.assertEquals(expectedData.get(expectedDataIndex)[0], city);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[1], year);
            Assert.assertEquals(expectedData.get(expectedDataIndex)[2], population);
            
            expectedDataIndex++;
        }
        
    }

}
