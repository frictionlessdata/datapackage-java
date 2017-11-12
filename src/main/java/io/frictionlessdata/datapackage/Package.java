package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.PackageException;
import java.io.BufferedReader;
import java.net.URL;
import org.json.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;
import org.everit.json.schema.ValidationException;

/**
 * Load, validate and create a datapackage object.
 * 
 */
public class Package {
    
    private static final String JSON_KEY_RESOURCES = "resources";
    private static final String JSON_KEY_NAME = "name";
    
    private String basePath = null;
    private JSONObject jsonObject = null;
    private Validator validator = new Validator();

    /**
     * Load from native Java JSONObject
     * @param jsonObjectSource 
     */
    public Package(JSONObject jsonObjectSource) throws ValidationException{
        // Validate data package JSON object before setting it.
        this.validator.validate(jsonObjectSource); // Will throw a ValidationException if JSON is not valid.
        this.jsonObject = jsonObjectSource;
    }
    
    /**
     * Load from JSON string.
     * @param jsonStringSource 
     */
    public Package(String jsonStringSource) throws ValidationException{
        // Validate data package JSON object before setting it.
        this.validator.validate(jsonStringSource); // Will throw a ValidationException if JSON is not valid.
        this.jsonObject = new JSONObject(jsonStringSource);
    }
    
    /**
     * Load from URL (must be in either 'http' or 'https' schemes).
     * @param urlSource 
     * @throws java.io.IOException 
     * @throws java.io.FileNotFoundException 
     */
    public Package(URL urlSource) throws ValidationException, IOException, FileNotFoundException{
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(urlSource.openStream()))) {
            StringBuilder builder = new StringBuilder();
            int read;
            char[] chars = new char[1024];
            while ((read = reader.read(chars)) != -1){
                builder.append(chars, 0, read); 
            }

            String jsonString = builder.toString();
            
            this.validator.validate(jsonString); // Will throw a ValidationException if JSON is not valid.
            this.jsonObject = new JSONObject(jsonString);
        }
    }
    
    /**
     * Load from local file system path.
     * @param filePath
     * @param basePath 
     * @throws org.everit.json.schema.ValidationException
     * @throws java.io.FileNotFoundException
     */
    public Package(String filePath, String basePath) throws ValidationException, FileNotFoundException {
        File sourceFile = null;
        
        if(StringUtils.isEmpty(basePath)){
            // There is no basePath, i.e. it is empty ("") or null.
            // Hence the source is the absolute path of the file.
            // In this case we grab the directory of the source path and st it as the basePath.
            sourceFile = new File(filePath);
                  
        }else{
            // There is a basePath. Construct the absolute path and load it.
            String absoluteFilePath = basePath + "/" + filePath;
            sourceFile = new File(absoluteFilePath);  
        }
        
        if(sourceFile.exists()){
            // Set base path
            this.basePath = sourceFile.getParent();

            // Read file, it should be a JSON.
            JSONObject sourceJsonObject = parseJsonString(sourceFile.getAbsolutePath());
            
            // Validate obtained data package JSON object before setting it.
            this.validator.validate(sourceJsonObject);
            this.jsonObject = sourceJsonObject;

        }else{
            throw new FileNotFoundException();
        }
    }
    
    public JSONObject getResource(String resourceName){
        JSONArray jsonArray = this.getJSONObject().getJSONArray(JSON_KEY_RESOURCES);
        
        for (int i = 0; i < jsonArray.length(); i++) {
            if(jsonArray.getJSONObject(i).getString(JSON_KEY_NAME).equalsIgnoreCase(resourceName)){
                return jsonArray.getJSONObject(i);
            }
        }
        
        return null;
    }
    
    public JSONArray getResources(){
        return this.getJSONObject().getJSONArray(JSON_KEY_RESOURCES);
    }
    
    public void addResource(JSONObject resource) throws ValidationException, PackageException{
        if(!resource.has(JSON_KEY_NAME)){
            throw new PackageException("The resource does not have a name property.");
        }
        
        String resourceName = resource.getString(JSON_KEY_NAME);
        
        JSONArray jsonArray = this.getJSONObject().getJSONArray(JSON_KEY_RESOURCES);
        
        for (int i = 0; i < jsonArray.length(); i++) {
            if(jsonArray.getJSONObject(i).getString(JSON_KEY_NAME).equalsIgnoreCase(resourceName)){
                throw new PackageException("A resource with the same name already exists.");
            }
        }
        this.getJSONObject().getJSONArray(JSON_KEY_RESOURCES).put(resource);
        
        this.validator.validate(this.getJSONObject());
    }
    
    public void removeResource(String name){
        JSONArray jsonArray = this.getJSONObject().getJSONArray(JSON_KEY_RESOURCES);
        
        for (int i = 0; i < jsonArray.length(); i++) {
            if(jsonArray.getJSONObject(i).getString(JSON_KEY_NAME).equalsIgnoreCase(name)){
                jsonArray.remove(i);
            }
        }
    }
    
    public void addData(String resourceName){
        throw new UnsupportedOperationException();
    }
    
    public void saveDescriptor(){
        throw new UnsupportedOperationException();
    }
    
    public void setTabularDataSchema(String resourceName){
        throw new UnsupportedOperationException();
    }
    
    public void revalidate() throws ValidationException{
        this.validator.validate(this.getJSONObject());
    }
    
    public String getBasePath(){
        return this.basePath;
    }
    
    public JSONObject getJSONObject(){
        return this.jsonObject;
    }
    
    private JSONObject parseJsonString(String absoluteFilePath) throws JSONException{
        // Read file, it should be a JSON.
        try{
            String jsonString = new String(Files.readAllBytes(Paths.get(absoluteFilePath)));
            return new JSONObject(jsonString);
            
        }catch(IOException ioe){
            // TODO: Come up with better exception handling?
            return null;
        }
    }
}
