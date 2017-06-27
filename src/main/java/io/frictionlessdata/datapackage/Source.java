package io.frictionlessdata.datapackage;

/**
 * The raw sources for this data package.
 * It MUST be an array of Source objects.
 * Each Source object MAY have title, path and email properties.
 * 
 * https://specs.frictionlessdata.io/data-package/#resource-information
 */
public class Source {
    private String title = null;
    private String path = null;
    private String email = null;
}
