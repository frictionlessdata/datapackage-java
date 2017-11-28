package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.validator.routines.UrlValidator;
import org.json.CDL;
import org.json.JSONArray;

/**
 * Resource.
 * Based on specs: http://frictionlessdata.io/specs/data-resource/
 */
public class Resource {
    
    // Data properties.
    private String path = null;
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
    
    // hashes and licenes?
    
    public final static String FORMAT_CSV = "csv";
    public final static String FORMAT_JSON = "json";
 
    public Resource(String name, String path){
        this.name = name;
        this.path = path;
    }
    
    public Resource(String name, Object data, String format){
        this.name = name;
        this.data = data;
        this.format = format;
    }

    public Iterator<CSVRecord> iter() throws IOException, FileNotFoundException, DataPackageException{
        // Error for non tabular
        if(!this.profile.equalsIgnoreCase(Profile.PROFILE_TABULAR_DATA_RESOURCE)){
            throw new DataPackageException("Unsupported for non tabular data.");
        }
        
        if(this.path != null){
            
            // First check if it's a URL String:
            String[] schemes = {"http", "https"};
            UrlValidator urlValidator = new UrlValidator(schemes);
            
            if (urlValidator.isValid(this.path)) {
                CSVParser parser = CSVParser.parse(new URL(this.path), Charset.forName("UTF-8"), CSVFormat.RFC4180);
                return parser.getRecords().iterator();
                
            }else{
                // If it's not a URL String, then it's a CSV String.
                Reader fr = new FileReader(this.path);
                return CSVFormat.RFC4180.parse(fr).iterator();
            }
               
        }else if (this.data != null){
            
            // Data is in String,  hence in CSV Format.
            if(this.data instanceof String && this.format.equalsIgnoreCase(FORMAT_CSV)){
                
                Reader sr = new StringReader((String)this.data);
                return CSVFormat.RFC4180.parse(sr).iterator();
                
            }
            // Data is not String, hence in JSON Array format.
            else if(this.data instanceof JSONArray && this.format.equalsIgnoreCase(FORMAT_JSON)){
                JSONArray dataJsonArray = (JSONArray)this.data;
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
    
    public void read() throws DataPackageException{
        if(!this.profile.equalsIgnoreCase(Profile.PROFILE_TABULAR_DATA_RESOURCE)){
            throw new DataPackageException("Unsupported for non tabular data.");
        }
        
        if(path != null){
            
        }else if (data != null){
            
        }else{
            throw new DataPackageException("No data has been set.");
        }
    }
    
    public String getProfile(){
        return this.profile;
    }
    
    public void setProfile(String profile){
        this.profile = profile;
    }
    
    public String getTitle(){
        return this.title;
    }
    
    public void setTitle(String title){
        this.title = title;
    }
    
    public String getDescription(){
        return this.description;
    }
    
    public void setDescription(String description){
        this.description = description;
    }
}
