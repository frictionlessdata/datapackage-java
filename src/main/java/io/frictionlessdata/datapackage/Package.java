package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
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
 */
public class Package {
    
    private static final String JSON_KEY_RESOURCES = "resources";
    private static final String JSON_KEY_NAME = "name";
    
    private String basePath = null;
    private JSONObject jsonObject = null;
    private Validator validator = new Validator();

    /**
     * Load from native Java JSONObject.
     * @param jsonObjectSource
     * @param strict
     * @throws ValidationException 
     */
    public Package(JSONObject jsonObjectSource, boolean strict) throws ValidationException{
        if(strict){
            // Validate data package JSON object before setting it.
            this.validator.validate(jsonObjectSource); // Will throw a ValidationException if JSON is not valid.
        }
        this.jsonObject = jsonObjectSource;
    }
    
    /**
     * Load from native Java JSONObject.
     * No validation by default.
     * @param jsonObjectSource 
     */
    public Package(JSONObject jsonObjectSource){
        this(jsonObjectSource, false);
    }
    
    /**
     * Load from JSON string.
     * @param jsonStringSource
     * @param strict
     * @throws ValidationException 
     */
    public Package(String jsonStringSource, boolean strict) throws ValidationException{
        if(strict){
           // Validate data package JSON object before setting it.
            this.validator.validate(jsonStringSource); // Will throw a ValidationException if JSON is not valid. 
        }
        
        this.jsonObject = new JSONObject(jsonStringSource);
    }
    
    /**
     * Load from JSON string.
     * No validation by default.
     * @param jsonStringSource 
     */
    public Package(String jsonStringSource){
        this(jsonStringSource, false);
    }
    
    /**
     * Load from URL (must be in either 'http' or 'https' schemes).
     * @param urlSource
     * @param strict
     * @throws ValidationException
     * @throws IOException
     * @throws FileNotFoundException 
     */
    public Package(URL urlSource, boolean strict) throws ValidationException, IOException, FileNotFoundException{
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(urlSource.openStream()))) {
            StringBuilder builder = new StringBuilder();
            int read;
            char[] chars = new char[1024];
            while ((read = reader.read(chars)) != -1){
                builder.append(chars, 0, read); 
            }

            String jsonString = builder.toString();
            
            if(strict){
                this.validator.validate(jsonString); // Will throw a ValidationException if JSON is not valid.
            }

            this.jsonObject = new JSONObject(jsonString);
        }
    }
    
    /**
     * Load from URL (must be in either 'http' or 'https' schemes).
     * No validation by default.
     * @param urlSource
     * @throws IOException
     * @throws FileNotFoundException 
     */
    public Package(URL urlSource) throws IOException, FileNotFoundException{
        this(urlSource, false);
    }
    
    /**
     * Load from local file system path.
     * @param filePath
     * @param basePath
     * @param strict
     * @throws ValidationException
     * @throws FileNotFoundException 
     */
    public Package(String filePath, String basePath, boolean strict) throws ValidationException, FileNotFoundException {
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
            
            if(strict){
                // Validate obtained data package JSON object before setting it.
                this.validator.validate(sourceJsonObject);
            }
            
            this.jsonObject = sourceJsonObject;
            
        }else{
            throw new FileNotFoundException();
        }
    }
    
    /**
     * Load from local file system path.
     * No validation by default.
     * @param filePath
     * @param basePath
     * @throws ValidationException
     * @throws FileNotFoundException 
     */
    public Package(String filePath, String basePath) throws FileNotFoundException {
        this(filePath, basePath, false); 
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
    
    public void addResource(JSONObject resource) throws ValidationException, DataPackageException{
        if(!resource.has(JSON_KEY_NAME)){
            throw new DataPackageException("The resource does not have a name property.");
        }
        
        String resourceName = resource.getString(JSON_KEY_NAME);
        
        JSONArray jsonArray = this.getJSONObject().getJSONArray(JSON_KEY_RESOURCES);
        
        for (int i = 0; i < jsonArray.length(); i++) {
            if(jsonArray.getJSONObject(i).getString(JSON_KEY_NAME).equalsIgnoreCase(resourceName)){
                throw new DataPackageException("A resource with the same name already exists.");
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
