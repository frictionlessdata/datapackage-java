package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.net.URL;
import org.json.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.everit.json.schema.ValidationException;

/**
 * Load, validate and create a datapackage object.
 */
public class Package {
    
    private static final int JSON_INDENT_FACTOR = 4;
    private static final String DATAPACKAGE_FILENAME = "datapackage.json";
    public static final String JSON_KEY_RESOURCES = "resources";
    public static final String JSON_KEY_NAME = "name";
    public static final String JSON_KEY_PROFILE = "profile";
    
    private String basePath = null;
    private JSONObject jsonObject = null;
    private boolean strictValidation = false;
    private List<Exception> errors = new ArrayList();
    private Validator validator = new Validator();
    
    public Package(){
    }

    /**
     * Load from native Java JSONObject.
     * @param jsonObjectSource
     * @param strict
     * @throws IOException
     * @throws DataPackageException
     * @throws ValidationException 
     */
    public Package(JSONObject jsonObjectSource, boolean strict) throws IOException, DataPackageException, ValidationException{ 
        this.jsonObject = jsonObjectSource;
        this.strictValidation = strict;
        
        this.validate();
    }
    
    /**
     * Load from native Java JSONObject.
     * @param jsonObjectSource
     * @throws IOException
     * @throws DataPackageException 
     */
    public Package(JSONObject jsonObjectSource) throws IOException, DataPackageException{
        this(jsonObjectSource, false);
    }
    
    /**
     * Load from String representation of JSON object or from a zip file path.
     * @param jsonStringSource
     * @param strict
     * @throws IOException
     * @throws DataPackageException
     * @throws ValidationException
     * @throws IOException 
     */
    public Package(String jsonStringSource, boolean strict) throws IOException, DataPackageException, ValidationException, IOException{
        this.strictValidation = strict;
        
        // If zip file is given.
        if(jsonStringSource.toLowerCase().endsWith(".zip")){
            // Read in memory the file inside the zip.
            ZipFile zipFile = new ZipFile(jsonStringSource);
            ZipEntry entry = zipFile.getEntry(DATAPACKAGE_FILENAME);
            
            // Throw exception if expected datapackage.json file not found.
            if(entry == null){
                throw new DataPackageException("The zip file does not contain the expected file: " + DATAPACKAGE_FILENAME);
            }
            
            // Read the datapackage.json file inside the zip
            try(InputStream is = zipFile.getInputStream(entry)){
                StringBuilder out = new StringBuilder();
                try(BufferedReader reader = new BufferedReader(new InputStreamReader(is))){
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        out.append(line);
                    }
                }
                
                // Create and set the JSONObject for the datapackage.json that was read from inside the zip file.
                this.jsonObject = new JSONObject(out.toString());  
                
                this.validate();
            }
   
        }else{
            // Create and set the JSONObject fpr the String representation of desriptor JSON object.
            this.jsonObject = new JSONObject(jsonStringSource); 
            
            // If String representation of desriptor JSON object is provided.
            this.validate(); 
        }
    }
    
    /**
     * Load from String representation of JSON object or from a zip file path.
     * @param jsonStringSource
     * @throws DataPackageException
     * @throws ValidationException
     * @throws IOException 
     */
    public Package(String jsonStringSource) throws DataPackageException, ValidationException, IOException{
        this(jsonStringSource, false);
    }
    
    /**
     * Load from URL (must be in either 'http' or 'https' schemes).
     * @param urlSource
     * @param strict
     * @throws DataPackageException
     * @throws ValidationException
     * @throws IOException
     * @throws FileNotFoundException 
     */
    public Package(URL urlSource, boolean strict) throws DataPackageException, ValidationException, IOException, FileNotFoundException{
        this.strictValidation = strict;
        
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(urlSource.openStream()))) {
            StringBuilder builder = new StringBuilder();
            int read;
            char[] chars = new char[1024];
            while ((read = reader.read(chars)) != -1){
                builder.append(chars, 0, read); 
            }

            String jsonString = builder.toString();
            
            this.jsonObject = new JSONObject(jsonString);
            this.validate();  
        }
    }
    
    /**
     * Load from URL (must be in either 'http' or 'https' schemes).
     * No validation by default.
     * @param urlSource
     * @throws DataPackageException
     * @throws IOException
     * @throws FileNotFoundException 
     */
    public Package(URL urlSource) throws DataPackageException, IOException, FileNotFoundException{
        this(urlSource, false);
    }
    
    /**
     * Load from local file system path.
     * @param filePath
     * @param basePath
     * @param strict
     * @throws DataPackageException
     * @throws ValidationException
     * @throws FileNotFoundException 
     */
    public Package(String filePath, String basePath, boolean strict) throws IOException, DataPackageException, ValidationException, FileNotFoundException {
        this.strictValidation = strict;
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
            
            this.jsonObject = sourceJsonObject;
            this.validate();

        }else{
            throw new FileNotFoundException();
        }
    }
    
    /**
     * Load from local file system path.
     * No validation by default.
     * @param filePath
     * @param basePath
     * @throws IOException
     * @throws DataPackageException
     * @throws FileNotFoundException 
     */
    public Package(String filePath, String basePath) throws IOException, DataPackageException, FileNotFoundException {
        this(filePath, basePath, false); 
    }
    
    public void save(String outputFilePath) throws IOException, DataPackageException{
        if(outputFilePath.toLowerCase().endsWith(".json")){
            this.saveJson(outputFilePath);
            
        }else if(outputFilePath.toLowerCase().endsWith(".zip")){
            this.saveZip(outputFilePath);
            
        }else{
            throw new DataPackageException("Unrecognized file format.");
        }
    }
    
    private void saveJson(String outputFilePath) throws IOException, DataPackageException{
        try (FileWriter file = new FileWriter(outputFilePath)) {
            file.write(this.getJson().toString(JSON_INDENT_FACTOR));
        }
    }
    
    private void saveZip(String outputFilePath) throws IOException, DataPackageException{
        try(FileOutputStream fos = new FileOutputStream(outputFilePath)){
            try(BufferedOutputStream bos = new BufferedOutputStream(fos)){
                try(ZipOutputStream zos = new ZipOutputStream(bos)){
                    // File is not on the disk, test.txt indicates
                    // only the file name to be put into the zip.
                    ZipEntry entry = new ZipEntry("datapackage.json"); 

                    zos.putNextEntry(entry);
                    zos.write(this.getJson().toString(JSON_INDENT_FACTOR).getBytes());
                    zos.closeEntry();
                }           
            }
        }
    }
    
    
    public void infer(){
        this.infer(false);
    }
    
    public void infer(boolean pattern){
        throw new UnsupportedOperationException();
    }
    
    public JSONObject getResource(String resourceName){
        JSONArray jsonArray = this.getJson().getJSONArray(JSON_KEY_RESOURCES);
        
        for (int i = 0; i < jsonArray.length(); i++) {
            if(jsonArray.getJSONObject(i).getString(JSON_KEY_NAME).equalsIgnoreCase(resourceName)){
                return jsonArray.getJSONObject(i);
            }
        }
        
        return null;
    }
    
    public JSONArray getResources(){
        return this.getJson().getJSONArray(JSON_KEY_RESOURCES);
    }
    
    public void addResource(JSONObject resource) throws IOException, ValidationException, DataPackageException{
        
        // If a name property isn't given...
        if(!resource.has(JSON_KEY_NAME)){
            DataPackageException dpe = new DataPackageException("The resource does not have a name property.");

            if(this.strictValidation){
                throw dpe;
            }else{
                errors.add(dpe);
            }
            
        }else{
            String resourceName = resource.getString(JSON_KEY_NAME);
            JSONArray jsonArray = this.getJson().getJSONArray(JSON_KEY_RESOURCES);

            // Check if there is duplication.
            for (int i = 0; i < jsonArray.length(); i++) {
                if(jsonArray.getJSONObject(i).getString(JSON_KEY_NAME).equalsIgnoreCase(resourceName)){
                    DataPackageException dpe = new DataPackageException("A resource with the same name already exists.");

                    if(this.strictValidation){
                        throw dpe;
                    }else{
                        errors.add(dpe);
                    }
                }
            }
        }
        
        // Validate.
        this.validate();
        
        this.getJson().getJSONArray(JSON_KEY_RESOURCES).put(resource);  
    }
    
    public void removeResource(String name){
        JSONArray jsonArray = this.getJson().getJSONArray(JSON_KEY_RESOURCES);
        
        for (int i = 0; i < jsonArray.length(); i++) {
            if(jsonArray.getJSONObject(i).getString(JSON_KEY_NAME).equalsIgnoreCase(name)){
                jsonArray.remove(i);
            }
        }
    }
    
    public Object getProperty(String key){
        return this.getJson().get(key);
    }
    
    public Object getPropertyString(String key){
        return this.getJson().getString(key);
    }
    
    public Object getPropertyJSONObject(String key){
        return this.getJson().getJSONObject(key);
    }
    
    public Object getPropertyJSONArray(String key){
        return this.getJson().getJSONArray(key);
    }
    
    public void addProperty(String key, String value) throws DataPackageException{
        if(this.getJson().has(key)){
            throw new DataPackageException("A property with the same key already exists.");
        }else{
            this.getJson().put(key, value);
        }
    }
    
    public void addProperty(String key, JSONObject value) throws DataPackageException{
        if(this.getJson().has(key)){
            throw new DataPackageException("A property with the same key already exists.");
        }else{
            this.getJson().put(key, value);
        }
    }
    
    public void addProperty(String key, JSONArray value) throws DataPackageException{
        if(this.getJson().has(key)){
            throw new DataPackageException("A property with the same key already exists.");
        }else{
            this.getJson().put(key, value);
        }
    }
    
    public void removeProperty(String key){
        this.getJson().remove(key);
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
    
    /**
     * Validation is strict or unstrict depending on how the package was
     * instanciated with the strict flag.
     * @throws IOException
     * @throws DataPackageException
     * @throws ValidationException 
     */
    public final void validate() throws IOException, DataPackageException, ValidationException{
        try{
            this.validator.validate(this.getJson());
            
        }catch(ValidationException ve){
            if(this.strictValidation){
                throw ve;
            }else{
                errors.add(ve);
            }
        }
    }
    
    public String getBasePath(){
        return this.basePath;
    }
    
    public JSONObject getJson(){
        return this.jsonObject;
    }
    
    public List<Exception> getErrors(){
        return this.errors;
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
