package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Iterator;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.validator.routines.UrlValidator;
import org.json.CDL;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Resource.
 * Based on specs: http://frictionlessdata.io/specs/data-resource/
 */
public class Resource {
    
    // Data properties.
    private Object path = null;
    private Object data = null;
    
    // Metadata properties.
    // Required properties.
    private String name = null;
    
    // Recommended properties.
    private String profile = null;
    
    // Optional properties.
    private String title = null;
    private String description = null;
    private String format = null;
    private String mediaType = null;
    private String encoding = null;
    private Integer bytes = null;
    private String hash = null;
    
    // hashes and licenses?
    
    // Schema
    private JSONObject schema = null;
    
    public final static String FORMAT_CSV = "csv";
    public final static String FORMAT_JSON = "json";
    
    // JSON keys.
    // FIXME: Use somethign like GSON instead so this explicit mapping is not
    // necessary?
    public final static String JSON_KEY_PATH = "path";
    public final static String JSON_KEY_DATA = "data";
    public final static String JSON_KEY_NAME = "name";
    public final static String JSON_KEY_PROFILE = "profile";
    public final static String JSON_KEY_TITLE = "title";
    public final static String JSON_KEY_DESCRIPTION = "description";
    public final static String JSON_KEY_FORMAT = "format";
    public final static String JSON_KEY_MEDIA_TYPE = "mediaType";
    public final static String JSON_KEY_ENCODING = "encoding";
    public final static String JSON_KEY_BYTES = "bytes";
    public final static String JSON_KEY_HASH = "hash";
    public final static String JSON_KEY_SCHEMA = "schema";
    
    public Resource(String name, Object path){
        this(name, path, new JSONObject());
    }
    
    public Resource(String name, Object path, JSONObject schema){
        this.name = name;
        this.path = path;
        if(schema.length() > 0){
            this.schema = schema;
        }
    }
        
    public Resource(String name, Object data, String format){
        this(name, data, format, null);
    }
    
    public Resource(String name, Object data, String format, JSONObject schema){
        this.name = name;
        this.data = data;
        this.format = format;
        this.schema = schema;
    }
    
    public Resource(String name, Object path, JSONObject schema, String profile, String title,
            String description, String mediaType,
            String encoding, Integer bytes, String hash){
        
        this.name = name;
        this.path = path;
        this.schema = schema;
        this.profile = profile;
        this.title = title;
        this.description = description;
        this.mediaType = mediaType;
        this.encoding = encoding;
        this.bytes = bytes;
        this.hash = hash;

    }
    
    public Resource(String name, Object data, String format, JSONObject schema, String profile,
            String title, String description, String mediaType,
            String encoding, Integer bytes, String hash){
        
        this.name = name;
        this.data = data;
        this.format = format;
        this.schema = schema;
        this.profile = profile;
        this.title = title;
        this.description = description;
        this.mediaType = mediaType;
        this.encoding = encoding;
        this.bytes = bytes;
        this.hash = hash;  
    }

    public Iterator<CSVRecord> iter() throws IOException, FileNotFoundException, DataPackageException{
        // Error for non tabular
        if(!this.profile.equalsIgnoreCase(Profile.PROFILE_TABULAR_DATA_RESOURCE)){
            throw new DataPackageException("Unsupported for non tabular data.");
        }
        
        // If the path of a data file has been set.
        if(this.getPath() != null){
            
            // And if it's just a one part resource (i.e. only one file path is given).
            if(this.getPath() instanceof String){
                // then just return the interator for the data located in that file
                return this.getIterator((String)this.getPath());
                
            }else if(this.getPath() instanceof JSONArray){ // If multipart resource (i.e. multiple file paths are given).
                
                // Create an iterator for each file, chain them, and then return them as a single iterator.
                JSONArray paths = ((JSONArray)this.getPath());
                Iterator<CSVRecord>[] interatorChain  = new Iterator[paths.length()];
                
                // Chain the iterators.
                for(int i = 0; i < paths.length(); i++){
                    interatorChain[i] = this.getIterator(paths.getString(i));
                }
                
                // Return the chained iterator.
                return new IteratorChain(interatorChain);
                
            }else{
                throw new DataPackageException("Unsupported data type for Resource path. Should be String or List but was " + this.getPath().getClass().getTypeName());
            }
               
        }else if (this.getData() != null){
            
            // Data is in String, hence in CSV Format.
            if(this.getData() instanceof String && this.getFormat().equalsIgnoreCase(FORMAT_CSV)){
                
                Reader sr = new StringReader((String)this.getData());
                return CSVFormat.RFC4180.parse(sr).iterator();
                
            }
            // Data is not String, hence in JSON Array format.
            else if(this.getData() instanceof JSONArray && this.getFormat().equalsIgnoreCase(FORMAT_JSON)){
                JSONArray dataJsonArray = (JSONArray)this.getData();
                String dataCsv = CDL.toString(dataJsonArray);
                
                Reader sr = new StringReader(dataCsv);
                return CSVFormat.RFC4180.parse(sr).iterator();
                
            }else{
                // Data is in unexpected format. Throw exception.
                throw new DataPackageException("A resource has an invalid data format. It should be a CSV String or a JSON Array.");
            }
            
        }else{
            throw new DataPackageException("No data has been set.");
        }
    }
    
    private Iterator<CSVRecord> getIterator(String path) throws IOException, MalformedURLException{
         
        String[] schemes = {"http", "https"};
        UrlValidator urlValidator = new UrlValidator(schemes);
                
        if (urlValidator.isValid(path)) {
            CSVParser parser = CSVParser.parse(new URL(path), Charset.forName("UTF-8"), CSVFormat.RFC4180);
            return parser.getRecords().iterator();

        }else{
            // If it's not a URL String, then it's a CSV String.
            
            // The path value can either be a relative path or a full path.
            // If it's a relative path then build the full path by using the working directory.
            File f = new File(path);
            if(!f.exists()) { 
                path = System.getProperty("user.dir") + "/" + path;
            }

            // Read the file.
            Reader fr = new FileReader(path);
            
            // Return iterator.
            return CSVFormat.RFC4180.parse(fr).iterator();
        }
    }
    
    public void read() throws DataPackageException{
        if(!this.profile.equalsIgnoreCase(Profile.PROFILE_TABULAR_DATA_RESOURCE)){
            throw new DataPackageException("Unsupported for non tabular data.");
        }
        
        if(this.getPath() != null){
            
        }else if (this.getData() != null){
            
        }else{
            throw new DataPackageException("No data has been set.");
        }
    }
    
    /**
     * Get JSON representation of the object.
     * @return 
     */
    public JSONObject getJson(){
        //FIXME: Maybe use something lke GSON so we don't have to explicitly
        //code this...
        JSONObject json = new JSONObject();
        
        // Null values will not actually be "put," as per JSONObject specs.
        json.put(JSON_KEY_NAME, this.getName());
        json.put(JSON_KEY_PATH, this.getPath());
        json.put(JSON_KEY_DATA, this.getData());
        json.put(JSON_KEY_PROFILE, this.getProfile());
        json.put(JSON_KEY_TITLE, this.getTitle());
        json.put(JSON_KEY_DESCRIPTION, this.getDescription());
        json.put(JSON_KEY_FORMAT, this.getFormat());
        json.put(JSON_KEY_MEDIA_TYPE, this.getMediaType());
        json.put(JSON_KEY_ENCODING, this.getEncoding());
        json.put(JSON_KEY_BYTES, this.getBytes());
        json.put(JSON_KEY_HASH, this.getHash());
        json.put(JSON_KEY_SCHEMA, this.getSchema());
        
        return json;
    }

    /**
     * @return the path
     */
    public Object getPath() {
        return path;
    }

    /**
     * @param path the path to set
     */
    public void setPath(Object path) {
        this.path = path;
    }

    /**
     * @return the data
     */
    public Object getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(Object data) {
        this.data = data;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the profile
     */
    public String getProfile() {
        return profile;
    }

    /**
     * @param profile the profile to set
     */
    public void setProfile(String profile) {
        this.profile = profile;
    }

    /**
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * @param title the title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return the format
     */
    public String getFormat() {
        return format;
    }

    /**
     * @param format the format to set
     */
    public void setFormat(String format) {
        this.format = format;
    }

    /**
     * @return the mediaType
     */
    public String getMediaType() {
        return mediaType;
    }

    /**
     * @param mediaType the mediaType to set
     */
    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }

    /**
     * @return the encoding
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * @param encoding the encoding to set
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /**
     * @return the bytes
     */
    public Integer getBytes() {
        return bytes;
    }

    /**
     * @param bytes the bytes to set
     */
    public void setBytes(Integer bytes) {
        this.bytes = bytes;
    }

    /**
     * @return the hash
     */
    public String getHash() {
        return hash;
    }

    /**
     * @param hash the hash to set
     */
    public void setHash(String hash) {
        this.hash = hash;
    }
    
    public JSONObject getSchema(){
        return this.schema;
    }
}
