package io.frictionlessdata.datapackage;

import java.util.ArrayList;

/**
 * The Data Resource format describes a data resource such as an individual file or table.
 * The essence of a Data Resource is a locator for the data it describes.
 * A range of other properties can be declared to provide a richer set of metadata.
 * 
 * https://specs.frictionlessdata.io/data-resource/
 */
public class Resource {
    public enum Format { csv, json };
    
    /**
     * A resource MUST contain a property describing the location of the data associated to the resource.
     * The location of resource data MUST be specified by the presence of one (and only one) of these two properties:
     */ 
    private Object path = null; // Path MUST be a string -- or an array of strings.  
    private Object data = null; // The value of the data property can be any type of data. However, restrictions of JSON require that the value be a string so for binary data you will need to encode (e.g. to Base64).
    
    // Required property
    private String name = null;
    
    // Recommended Properties
    private Profile profile = null;
    
    // Optional Properties
    private String title = null;
    private String description = null;
    private Format format = null;
    private String mediatype = null;
    private String encoding = null;
    private int bytes;
    private String hash = null;
    
    private ArrayList<Source> sources = null;
    private ArrayList<License> licenses = null;
    
    /**
     * The value for the schema property on a resource:
     *  - MUST be an object representing the schema
     *  - OR a string that identifies the location of the schema.
     * 
     * If a string it must be a url-or-path as defined above,
     * that is a fully qualified http URL or a relative POSIX path.
     * The file at the the location specified by this url-or-path string
     * MUST be a JSON document containing the schema.
     */
    private Object schema = null;
    
}
