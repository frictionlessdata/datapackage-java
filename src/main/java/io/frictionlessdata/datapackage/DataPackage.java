package io.frictionlessdata.datapackage;

import java.util.ArrayList;

/**
 * Class for loading, validating and working with a Data Package.
 */
public class DataPackage {
    // The descriptor is the central file in a Data Package
    private String descriptor = null;
    
    // Packaged data resources are described in the resources property of the package descriptor. 
    private ArrayList<Resource> resources = null;
    
    // Recommended properties
    private String name = null;
    private String id = null;
    private ArrayList<License> licenses = null;
    private Profile profile = null;
    
    // Option properties
    private String title = null;
    private String description = null;
    private String homepage = null;
    private String version = null;
    
    private ArrayList<Source> sources = null;
    private ArrayList<Contributor> contributors = null;
    private ArrayList<String> keywords = null;
    
    private String image = null;
    private String created = null; //FIXME: Should probably use some sort of datetime type.
    
    
    public DataPackage(String descriptor) {
        this.descriptor = descriptor;
    }
}
