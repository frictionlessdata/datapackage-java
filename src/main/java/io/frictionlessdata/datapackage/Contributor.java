package io.frictionlessdata.datapackage;

/**
 * The people or organizations who contributed to this Data Package.
 * It MUST be an array.
 * Each entry is a Contributor and MUST be an object.
 * A Contributor MUST have a name property and MAY contain path, email, role and organization properties.
 * 
 * https://specs.frictionlessdata.io/data-package/#resource-information
 */
public class Contributor {
    
    public enum Role { author, publisher, maintainer, wrangler, contributor };
    
    private String title = null;
    private String path = null;
    private String email = null;
    private Role role = null;

}
