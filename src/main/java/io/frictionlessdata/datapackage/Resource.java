package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

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
 
    public Resource(String name, String path){
        this.name = name;
        this.path = path;
    }
    
    public Resource(String name, Object data, String format){
        this.name = name;
        this.data = data;
        this.format = format;
    }

    public Iterable<CSVRecord> iter() throws IOException, FileNotFoundException, DataPackageException{
        // Error for non tabular
        if(this.profile.equalsIgnoreCase(Profile.PROFILE_TABULAR_DATA_RESOURCE)){
            throw new DataPackageException("Unsupported for non tabular data.");
        }
        
        if(this.path != null){
            Reader in = new FileReader(this.path);
            Iterable<CSVRecord> csvRecords = CSVFormat.RFC4180.parse(in);
            
            return csvRecords;
            
        }else if (this.data != null){
            return null;
            
        }else{
            throw new DataPackageException("No data has been set.");
        }
    }
    
    public void read() throws DataPackageException{
        if(this.profile.equalsIgnoreCase(Profile.PROFILE_TABULAR_DATA_RESOURCE)){
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
