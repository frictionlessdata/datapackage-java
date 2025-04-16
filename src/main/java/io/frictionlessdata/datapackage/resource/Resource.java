package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.frictionlessdata.datapackage.Package;
import io.frictionlessdata.datapackage.*;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.exception.TypeInferringException;
import io.frictionlessdata.tableschema.iterator.TableIterator;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static io.frictionlessdata.datapackage.JSONBase.JSON_KEY_DATA;
import static io.frictionlessdata.datapackage.Validator.isValidUrl;


/**
 * Interface for a Resource. The essence of a Data Resource is a locator for the data it describes.
 * A range of other properties can be declared to provide a richer set of metadata.
 *
 * Based on specs: http://frictionlessdata.io/specs/data-resource/
 */
@JsonInclude(value= JsonInclude.Include. NON_EMPTY, content= JsonInclude.Include. NON_NULL)
public interface Resource<T> extends BaseInterface {

    String FORMAT_CSV = "csv";
    String FORMAT_JSON = "json";

    /**
     * Return the {@link Table} objects underlying the Resource.
     * @return Table(s)
     * @throws Exception if reading the tables fails.
     */
    @JsonIgnore
    List<Table> getTables() throws Exception ;

    /**
     * Read all data from a Resource, unmapped and not transformed. This is useful for non-tabular resources
     *
     * @return Contents of the resource file or URL.
     * @throws IOException if reading the data fails
     *
     */
    @JsonProperty(JSON_KEY_DATA)
    public Object getRawData() throws IOException;

    /**
     * Read all data from a Resource, each row as String arrays. This can be used for smaller datapackages,
     * but for huge or unknown sizes, reading via iterator  is preferred, as this method loads all data into RAM.
     *
     * It can be configured to return table rows with relations to other data sources resolved
     *
     * The method uses Iterators provided by {@link Table} class, and is roughly implemented after
     * https://github.com/frictionlessdata/tableschema-py/blob/master/tableschema/table.py
     *
     * @param relations true: follow relations
     * @return A list of table rows.
     * @throws Exception if parsing the data fails
     *
     */
    @JsonIgnore
    public List<String[]> getData(boolean relations) throws Exception;

    /**
     * Read all data from a Resource, each row as Map objects. This can be used for smaller datapackages,
     * but for huge or unknown sizes, reading via iterator  is preferred, as this method loads all data into RAM.
     *
     * The method returns Map&lt;String,Object&gt; where key is the header name, and val is the data.
     * It can be configured to return table rows with relations to other data sources resolved
     *
     * The method uses Iterators provided by {@link Table} class, and is roughly implemented after
     * https://github.com/frictionlessdata/tableschema-py/blob/master/tableschema/table.py
     *
     * @param relations true: follow relations
     * @return A list of table rows.
     * @throws Exception if parsing the data fails
     *
     */
    List<Map<String, Object>> getMappedData(boolean relations) throws Exception;

    /**
     * Most customizable method to retrieve all data in a Resource. Parameters match those in
     * {@link io.frictionlessdata.tableschema.Table#iterator(boolean, boolean, boolean, boolean)}.
     * This can be used for smaller datapackages, but for huge or unknown
     * sizes, reading via iterator  is preferred, as this method loads all data into RAM.
     *
     * The method can be configured to return table rows as:
     * <ul>
     *     <li>String arrays  (parameter `cast` = false)</li>
     *     <li>as Object arrays (parameter `cast` = true)</li>
     *     <li>as a Map&lt;String,Object&gt; where key is the header name, and val is
     *          the data (parameter `keyed` = true)</li>
     *     <li>in an "extended" form (parameter `extended` = true) that returns an Object array where the first entry
     *      is the row number, the second is a String array holding the headers,
     *      and the third is an Object array holding the row data.</li>
     *      <li>with relations to other data sources resolved</li>
     * </ul>
     *
     * The method uses Iterators provided by {@link Table} class, and is roughly implemented after
     * https://github.com/frictionlessdata/tableschema-py/blob/master/tableschema/table.py
     *
     * @param keyed true: return table rows as key/value maps
     * @param extended true: return table rows in an extended form
     * @param cast true: convert CSV cells to Java objects other than String
     * @param relations true: follow relations
     * @return A list of table rows.
     * @throws Exception if parsing the data fails
     *
     */
    List<Object> getData(boolean keyed, boolean extended, boolean cast, boolean relations) throws Exception;

    /**
     * Read all data from a Resource. This can be used for smaller datapackages, but for huge or unknown
     * sizes, reading via iterator  is preferred, as this method loads all data into RAM.
     * The method ignores relations.
     *
     * Returns as a List of Java objects of the type `beanClass`. Under the hood, it uses a  {@link TableIterator}
     * for reading based on a Java Bean class instead of a {@link io.frictionlessdata.tableschema.schema.Schema}.
     * It therefore disregards the Schema set on the {@link io.frictionlessdata.tableschema.Table} the iterator works
     * on but creates its own Schema from the supplied `beanType`.
     *
     * @return List of rows as bean instances.
     * @param beanClass the Bean class this BeanIterator expects
     */
    <C> List<C> getData(Class<C> beanClass) throws Exception;

    /**
     * Read all data from all Tables and return it as JSON.
     *
     * It ignores relations to other data sources.
     *
     * @return A JSON representation of the data as a String.
     */
    @JsonIgnore
    String getDataAsJson();

    /**
     * Read all data from all Tables and return it as a String in the format of the Resource's Dialect.
     * Column order will be deducted from the table data source.
     *
     * @return A CSV representation of the data as a String.
     */
    @JsonIgnore
    String getDataAsCsv();

    /**
     * Return the data of all Tables as a CSV string,
     *
     * - the `dialect` parameter decides on the CSV options. If it is null, then the file will
     *    be written as RFC 4180 compliant CSV
     * - the `schema` parameter decides on the order of the headers in the CSV file. If it is null,
     *    the Schema of the Resource will be used, or if none, the order of the columns will be
     *    the same as in the Tables.
     *
     * It ignores relations to other data sources.
     *
     * @param dialect the CSV dialect to use
     * @param schema a Schema defining header row names in the order in which data should be exported
     *
     * @return A CSV representation of the data as a String.
     */
    String getDataAsCsv(Dialect dialect, Schema schema);

    /**
     * Write all the data in this resource into one or more
     * files inside `outputDir`, depending on how many tables this
     * Resource holds.
     *
     * @param outputDir the directory to write to. Code must create
     *                  files as needed.
     * @throws Exception if something fails while writing
     */
    void writeData(Path outputDir) throws Exception;

    /**
     * Write all the data in this resource into the provided {@link Writer}.
     *
     * @param out the writer to write to.
     * @throws Exception if something fails while writing
     */
    void writeData(Writer out) throws Exception;

    /**
     * Write the Resource {@link Schema} to `outputDir`.
     *
     * @param parentFilePath the directory to write to. Code must create
     *                  files as needed.
     * @throws IOException if something fails while writing
     */
    void writeSchema(Path parentFilePath) throws IOException;

    /**
     * Write the Resource {@link Dialect} to `outputDir`.
     *
     * @param parentFilePath the directory to write to. Code must create
     *                  files as needed.
     * @throws IOException if something fails while writing
     */
    void writeDialect(Path parentFilePath) throws IOException;

    /**
     * Returns an Iterator that returns rows as object-arrays. Values in each column
     * are parsed and converted ("cast") to Java objects based on the Field definitions of the Schema.
     * @return Iterator returning table rows as Object Arrays
     * @throws Exception  if parsing the data fails
     */
    Iterator<Object[]> objectArrayIterator() throws Exception;

    /**
     * Returns an Iterator that returns rows as object-arrays. Values in each column
     *    are parsed and converted ("cast") to Java objects based on the Field definitions of the Schema.
     * @return Iterator returning table rows as Object Arrays
     * @throws Exception if parsing the data fails
     */
    Iterator<Object[]> objectArrayIterator(boolean extended, boolean relations) throws Exception;

    /**
     * Returns an Iterator that returns rows as a Map&lt;key,val&gt; where key is the header name, and val is the data.
     * It can be configured to follow relations
     *
     * @param relations Whether references to other data sources get resolved
     * @return Iterator that returns rows as Maps.
     * @throws Exception if parsing the data fails
     */
    Iterator<Map<String, Object>> mappingIterator(boolean relations) throws Exception;

    /**
     * Returns an Iterator that returns rows as bean-arrays.
     * {@link TableIterator} based on a Java Bean class instead of a {@link io.frictionlessdata.tableschema.schema.Schema}.
     * It therefore disregards the Schema set on the {@link io.frictionlessdata.tableschema.Table} the iterator works
     * on but creates its own Schema from the supplied `beanType`.
     *
     * @return Iterator that returns rows as bean-arrays.
     * @param beanType the Bean class this BeanIterator expects
     * @param relations follow relations to other data source
     */
    <C> Iterator<C> beanIterator(Class<C> beanType, boolean relations)throws Exception;

    /**
     * This method creates an Iterator that will return table rows as String arrays.
     * It therefore disregards the Schema set on the table. It does not follow relations.
     *
     * @return Iterator that returns rows as string arrays.
     */
    public Iterator<String[]> stringArrayIterator() throws Exception;

    /**
     * This method creates an Iterator that will return table rows as String arrays.
     * It therefore disregards the Schema set on the table. It can be configured to follow relations.
     *
     * @return Iterator that returns rows as string arrays.
     */
    public Iterator<String[]> stringArrayIterator(boolean relations) throws Exception;


    String[] getHeaders() throws Exception;

    /**
     * Construct a path to write out the Schema for this Resource
     * @return a String containing a relative path for writing or null
     */
    String getPathForWritingSchema();

    /**
     * Construct a path to write out the Dialect for this Resource
     * @return a String containing a relative path for writing or null
     */
    String getPathForWritingDialect();

    /**
     * Return a set of relative path names we would use if we wanted to write
     * the resource data to file. For DataResources, this helps with conversion
     * to FileBasedResources
     * @return Set of relative path names
     */
    Set<String> getDatafileNamesForWriting();

    /**
     * @return the dialect
     */
    Dialect getDialect();

    /**
     * @param dialect the dialect to set
     */
    void setDialect(Dialect dialect);

    /**
     * Returns the Resource format, either "csv" or "json"
     * @return the format of this Resource
     */
    String getFormat();

    /**
     * Sets the Resource format, either "csv" or "json"
     * @param format the format to set
     */
    void setFormat(String format);

    String getDialectReference();

    Schema getSchema();

    void setSchema(Schema schema);

    public Schema inferSchema() throws TypeInferringException;

    @JsonIgnore
    boolean shouldSerializeToFile();

    @JsonIgnore
    void setShouldSerializeToFile(boolean serializeToFile);

    /**
     * Sets the format (either CSV or JSON) for serializing the Resource content to File.
     * @param format either FORMAT_CSV or FORMAT_JSON, other strings will cause an Exception
     */
    void setSerializationFormat(String format);

    String getSerializationFormat();

    void checkRelations(Package pkg) throws Exception;

    /**
     * Recreate a Resource object from a JSON descriptor, a base path to resolve relative file paths against
     * and a flag that tells us whether we are reading from inside a ZIP archive.
     *
     * @param resourceJson JSON descriptor containing properties like `name, `data` or `path`
     * @param basePath File system path used to resolve relative path entries if `path` contains entries
     * @param isArchivePackage  true if we are reading files from inside a ZIP archive.
     * @return fully inflated Resource object. Subclass depends on the data found
     * @throws IOException thrown if reading data failed
     * @throws DataPackageException for invalid data
     * @throws Exception if other operation fails.
     */

    static AbstractResource build(
            ObjectNode resourceJson,
            Object basePath,
            boolean isArchivePackage) throws IOException, DataPackageException, Exception {
        String name = textValueOrNull(resourceJson, JSONBase.JSON_KEY_NAME);
        Object path = resourceJson.get(JSONBase.JSON_KEY_PATH);
        Object data = resourceJson.get(JSON_KEY_DATA);
        String format = textValueOrNull(resourceJson, JSONBase.JSON_KEY_FORMAT);
        String profile = textValueOrNull(resourceJson, JSONBase.JSON_KEY_PROFILE);
        Dialect dialect = JSONBase.buildDialect (resourceJson, basePath, isArchivePackage);
        Schema schema = JSONBase.buildSchema(resourceJson, basePath, isArchivePackage);
        String encoding = textValueOrNull(resourceJson, JSONBase.JSON_KEY_ENCODING);
        Charset charset = TableDataSource.getDefaultEncoding();
        if (StringUtils.isNotEmpty(encoding)) {
            charset = Charset.forName(encoding);
        }

        // Now we can build the resource objects
        AbstractResource resource = null;

        if (path != null){
            Collection paths = fromJSON(path, basePath);
            resource = build(name, paths, basePath, charset);
            if (resource instanceof FilebasedResource) {
                ((FilebasedResource)resource).setIsInArchive(isArchivePackage);
            }
            // inlined data
        } else if (data != null){
            if (null == format) {
                resource = buildJsonResource(data, name, null, profile);
            } else if (format.equals(Resource.FORMAT_JSON))
                resource = buildJsonResource(data, name, format, profile);
            else if (format.equals(Resource.FORMAT_CSV)) {
                // data is in inline CSV format like "data": "A,B,C\n1,2,3\n4,5,6"
                String dataString = ((TextNode)data).textValue().replaceAll("\\\\n", "\n");
                resource = new CSVDataResource(name, dataString);
            }
        } else {
            throw new DataPackageValidationException(
                    "Invalid Resource. The path property or the data and format properties cannot be null.");
        }
        resource.setDialect(dialect);
        JSONBase.setFromJson(resourceJson, resource);
        resource.setSchema(schema);
        return resource;
    }

    private static AbstractResource buildJsonResource(Object data, String name, String format, String profile) {
        AbstractResource resource = null;
        if ((data instanceof ArrayNode)) {
            resource = new JSONDataResource(name, (ArrayNode)data);
        } else {
            if ((null != profile) && profile.equalsIgnoreCase(Profile.PROFILE_TABULAR_DATA_RESOURCE) && (StringUtils.isEmpty(format))) {
                // from the spec: " a JSON string - in this case the format or
                // mediatype properties MUST be provided
                // https://specs.frictionlessdata.io/data-resource/#data-inline-data
                throw new DataPackageValidationException(
                        "Invalid Resource. The format property cannot be null for inlined CSV data.");
            } else if ((data instanceof ObjectNode)) {
                resource = new JSONObjectResource(name, (ObjectNode)data);
            } else {
                throw new DataPackageValidationException(
                        "Invalid Resource. No implementation for inline data of type " + data.getClass().getSimpleName());
            }
        }
        return resource;
    }

    static AbstractResource build(String name, Collection<?> pathOrUrl, Object basePath, Charset encoding)
            throws MalformedURLException {
        if (pathOrUrl != null) {
            List<File> files = new ArrayList<>();
            List<URL> urls = new ArrayList<>();
            List<String> strings = new ArrayList<>();
            for (Object o : pathOrUrl) {
                if (o instanceof File) {
                    files.add((File)o);
                } else if (o instanceof Path) {
                    files.add(((Path)o).toFile());
                } else if (o instanceof URL) {
                    urls.add((URL)o);
                } else if (o instanceof TextNode) {
                    strings.add(o.toString());
                } else {
                    throw new IllegalArgumentException("Cannot build a resource out of "+o.getClass());
                }
            };

            // we have some relative paths, now lets find out whether they are URL fragments
            // or relative file paths
            for (String s : strings) {
                if (basePath instanceof URL) {
                    /*
                     * We have a URL fragment that is not valid on its own.
                     * According to https://github.com/frictionlessdata/specs/issues/652 ,
                     * URL fragments should be resolved relative to the base URL
                     */
                    URL f = new URL(((URL)basePath), s);
                    urls.add(f);
                } else if (isValidUrl(s)) {
                    URL f = new URL(s);
                    urls.add(f);
                } else {
                    File f = new File(s);
                    files.add(f);
                }
            };

            /*
                From the spec: "It is NOT permitted to mix fully qualified URLs and relative paths
                in a path array: strings MUST either all be relative paths or all URLs."

                https://frictionlessdata.io/specs/data-resource/index.html#data-in-multiple-files
             */
            if (!files.isEmpty() && !urls.isEmpty()) {
                throw new DataPackageException("Resources with mixed URL/File paths are not allowed");
            } else if (!files.isEmpty()) {
                return new FilebasedResource(name, files, normalizePath(basePath), encoding);
            } else if (!urls.isEmpty()) {
                return new URLbasedResource(name, urls);
            }
        }
        return null;
    }

    /**
     * return a File for the basePath object, no matter whether it is a String,
     * Path, or File
     * @param basePath Input path object
     * @return File pointing to the location in `basePath`
     */
    static File normalizePath(Object basePath) {
        if (basePath instanceof Path) {
            return ((Path)basePath).toFile();
        } else if (basePath instanceof String) {
            return new File((String) basePath);
        } else {
            return (File) basePath;
        }
    }

    static Collection fromJSON(Object path, Object basePath) throws IOException {
        if (null == path)
            return null;
        if (path instanceof ArrayNode) {
            return fromJSON((ArrayNode) path, basePath);
        } else if (path instanceof TextNode) {
        	return fromJSON(JsonUtil.getInstance().createArrayNode().add((TextNode)path), basePath);
        } else {
            return Collections.singleton(path);
        }
    }

    static Collection fromJSON(ArrayNode arr, Object basePath) throws IOException {
        if (null == arr)
            return null;
        Collection dereferencedObj = new ArrayList();

        for (JsonNode obj : arr) {
            if (!(obj.isTextual()))
                throw new IllegalArgumentException("Cannot dereference a "+obj.getClass());
            String location = obj.asText();
            if (isValidUrl(location)) {
                /*
                    This is a fully qualified URL "https://somesite.com/data/cities.csv".
                 */
                dereferencedObj.add(new URL(location));
            } else {
                if (basePath instanceof Path) {
                    /*
                        relative path, store for later dereferencing.
                        For reading, must be read relative to the basePath
                     */
                    dereferencedObj.add(new File(location));
                } else if (basePath instanceof URL) {
                    /*
                        This is a URL fragment "data/cities.csv".
                        According to https://github.com/frictionlessdata/specs/issues/652,
                        it should be parsed against the base URL (the Descriptor URL)
                     */
                    dereferencedObj.add(new URL(((URL)basePath),location));
                }
            }
        }
        return dereferencedObj;
    }
    //https://docs.oracle.com/javase/tutorial/essential/io/pathOps.html
    static Path toSecure(Path testPath, Path referencePath) throws IOException {
        // catch paths starting with "/" but on Windows where they get rewritten
        // to start with "\"
        if (testPath.startsWith(File.separator))
            throw new IllegalArgumentException("Input path must be relative");
        if (testPath.isAbsolute()){
            throw new IllegalArgumentException("Input path must be relative");
        }
        if (!referencePath.isAbsolute()) {
            throw new IllegalArgumentException("Reference path must be absolute");
        }
        if (testPath.toFile().isDirectory()){
            throw new IllegalArgumentException("Input path cannot be a directory");
        }
        //Path canonicalPath = testPath.toRealPath(null);
        final Path resolvedPath = referencePath.resolve(testPath).normalize();
        if (!Files.exists(resolvedPath))
            throw new FileNotFoundException("File "+resolvedPath.toString()+" does not exist");
        if (!resolvedPath.toFile().isFile()){
            throw new IllegalArgumentException("Input must be a file");
        }
        if (!resolvedPath.startsWith(referencePath)) {
            throw new IllegalArgumentException("Input path escapes the base path");
        }

        return resolvedPath;
    }

    static String textValueOrNull(JsonNode source, String fieldName) {
    	return source.has(fieldName) ? source.get(fieldName).asText() : null;
    }

    void validate(Package pkg);
}