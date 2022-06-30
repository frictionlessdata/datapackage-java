package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;

public class ContributorTest {
	
	String validContributorsJson = 
			"[{" + 
			"\"title\":\"HDIC\"," + 
			"\"email\":\"me@example.com\"," + 
			"\"path\":\"https://example.com\"," + 
			"\"role\":\"AUTHOR\"," + 
			"\"organization\":\"space cadets\"" + 
			"}]";
	
	String invalidPathContributorsJson = 
			"[{" + 
			"\"title\":\"HDIC\"," + 
			"\"email\":\"me@example.com\"," + 
			"\"path\":\"qwerty\"," + 
			"\"role\":\"AUTHOR\"," + 
			"\"organization\":\"space cadets\"" + 
			"}]";
	
	String invalidRoleContributorsJson = 
			"[{" + 
			"\"title\":\"HDIC\"," + 
			"\"email\":\"me@example.com\"," + 
			"\"path\":\"https://example.com\"," + 
			"\"role\":\"ERTYUIJHG\"," + 
			"\"organization\":\"space cadets\"" + 
			"}]";

    @Test
    @DisplayName("validate serialization and deserialization of a contributor object")
    public void testSerialization() {
    	Collection<Contributor> contributors = Contributor.fromJson(validContributorsJson);
		JsonUtil instance = JsonUtil.getInstance();
		String actual = instance.serialize(contributors, false);
    	Assertions.assertEquals(validContributorsJson, actual);
    }
    
    @Test
    @DisplayName("validate DPE is thrown with invalid url")
    public void testInvalidPath() {
    	DataPackageException ex = Assertions.assertThrows(DataPackageException.class, ()->{
    		Contributor.fromJson(invalidPathContributorsJson);
    	});
    	Assertions.assertEquals(Contributor.invalidUrlMsg, ex.getMessage());
    }
    
    @Test
    @DisplayName("validate DPE is thrown with invalid Role")
    public void testInvalidRole() {
    	DataPackageException ex = Assertions.assertThrows(DataPackageException.class, ()->{
    		Contributor.fromJson(invalidRoleContributorsJson);
    	});
    	Assertions.assertTrue(ex.getMessage().contains("\"ERTYUIJHG\": not one of the values accepted"));
    }
}
