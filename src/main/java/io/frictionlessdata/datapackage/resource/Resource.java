package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.JSONBase;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.iterator.TableIterator;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static io.frictionlessdata.datapackage.Package.isValidUrl;


/**
 * Interface for a Resource.
 * Based on specs: http://frictionlessdata.io/specs/data-resource/
 */
public interface Resource<T,C> {

    String FORMAT_CSV = "csv";
    String FORMAT_JSON = "json";

    List<Table> getTables() throws Exception ;

    JSONObject getJson();

    List<Object[]> read (boolean cast) throws Exception;

    List<C> read (Class<C> beanClass) throws Exception;

    /**
     * Write all the data in this resource as CSV into one or more
     * files inside `outputDir`, depending on how many tables this
     * Resource holds.
     *
     * @param outputDir the directory to write to. Code must create
     *                  files as needed.
     * @param dialect the CSV dialect to use for writing
     * @throws Exception if something fails while writing
     */
    void writeDataAsCsv(Path outputDir, Dialect dialect) throws Exception;

    /**
     * Write the Table as CSV into a file inside `outputDir`.
     *
     * @param outputFile the file to write to.
     * @param dialect the CSV dialect to use for writing
     * @throws Exception if something fails while writing
     */
    void writeTableAsCsv(Table table, Dialect dialect, Path outputFile) throws Exception;

    /**
     * Returns an Iterator that returns rows as object-arrays
     * @return
     * @throws Exception
     */
    Iterator<Object[]> objectArrayIterator() throws Exception;

    /**
     * Returns an Iterator that returns rows as object-arrays
     * @return
     * @throws Exception
     */
    Iterator<Object[]> objectArrayIterator(boolean keyed, boolean extended, boolean cast, boolean relations) throws Exception;

    /**
     * Returns an Iterator that returns rows as string-arrays
     * @return
     * @throws Exception
     */
    public Iterator<String[]> stringArrayIterator() throws Exception;

    String[] getHeaders() throws Exception;

    /**
     * @return the name
     */
    String getName();

    /**
     * @param name the name to set
     */
    void setName(String name);

    /**
     * @return the profile
     */
    String getProfile();

    /**
     * @param profile the profile to set
     */
    void setProfile(String profile);

    /**
     * @return the title
     */
    String getTitle();

    /**
     * @param title the title to set
     */
    void setTitle(String title);

    /**
     * @return the description
     */
    String getDescription();

    /**
     * @param description the description to set
     */
    void setDescription(String description);


    /**
     * @return the mediaType
     */
    String getMediaType();

    /**
     * @param mediaType the mediaType to set
     */
    void setMediaType(String mediaType);

    /**
     * @return the encoding
     */
    String getEncoding();

    /**
     * @param encoding the encoding to set
     */
    void setEncoding(String encoding);

    /**
     * @return the bytes
     */
    Integer getBytes();

    /**
     * @param bytes the bytes to set
     */
    void setBytes(Integer bytes);

    /**
     * @return the hash
     */
    String getHash();

    /**
     * @param hash the hash to set
     */
    void setHash(String hash);

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

    String getSchemaReference();

    String getDialectReference();

    Schema getSchema();

    void setSchema(Schema schema);

    /**
     * @return the sources
     */
    JSONArray getSources();

    /**
     * @param sources the sources to set
     */
    void setSources(JSONArray sources);

    /**
     * @return the licenses
     */
    JSONArray getLicenses();

    /**
     * @param licenses the licenses to set
     */
    void setLicenses(JSONArray licenses);

    boolean shouldSerializeToFile();

    void setShouldSerializeToFile(boolean serializeToFile);


    static AbstractResource build(JSONObject resourceJson, Object basePath, boolean isArchivePackage) throws IOException, DataPackageException, Exception {
        String name = resourceJson.has(JSONBase.JSON_KEY_NAME) ? resourceJson.getString(JSONBase.JSON_KEY_NAME) : null;
        Object path = resourceJson.has(JSONBase.JSON_KEY_PATH) ? resourceJson.get(JSONBase.JSON_KEY_PATH) : null;
        Object data = resourceJson.has(JSONBase.JSON_KEY_DATA) ? resourceJson.get(JSONBase.JSON_KEY_DATA) : null;
        String format = resourceJson.has(JSONBase.JSON_KEY_FORMAT) ? resourceJson.getString(JSONBase.JSON_KEY_FORMAT) : null;
        Dialect dialect = JSONBase.buildDialect (resourceJson, basePath, isArchivePackage);
        Schema schema = JSONBase.buildSchema(resourceJson, basePath, isArchivePackage);

        // Now we can build the resource objects
        AbstractResource resource = null;

        if (path != null){
            Collection paths = fromJSON(path, basePath);
            resource = build(name, paths, basePath);
            if (resource instanceof FilebasedResource) {
                ((FilebasedResource)resource).setIsInArchive(isArchivePackage);
            }
        } else if (data != null && format != null){
            if (format.equals(Resource.FORMAT_JSON))
                resource = new JSONDataResource(name, ((JSONArray) data).toString());
            else if (format.equals(Resource.FORMAT_CSV))
                resource = new CSVDataResource(name, (String)data);
        } else {
            DataPackageException dpe = new DataPackageException(
                    "Invalid Resource. The path property or the data and format properties cannot be null.");
            throw dpe;
        }
        resource.setDialect(dialect);
        JSONBase.setFromJson(resourceJson, resource, schema);
        return resource;
    }


    static AbstractResource build(String name, Collection pathOrUrl, Object basePath) throws MalformedURLException {
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
                } else if (o instanceof String) {
                    strings.add((String)o);
                } else {
                    throw new IllegalArgumentException("Cannot build a resource out of "+o.getClass());
                }
            };

            // we have some relative paths, now lets find out whether they are URL fragments
            // or relative file paths
            for (String s : strings) {
                if (basePath instanceof URL) {
                    /*
                     * We have a URL fragment, that is not valid on its own.
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
                return new FilebasedResource(name, files, normalizePath(basePath));
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
        Collection paths;
        if (path instanceof JSONArray) {
            paths = fromJSON((JSONArray) path, basePath);
        } else {
            paths = new ArrayList();
            paths.add(path);
        }
        return paths;
    }

    static Collection fromJSON(JSONArray arr, Object basePath) throws IOException {
        if (null == arr)
            return null;
        Collection dereferencedObj = new ArrayList();

        for (Object obj : arr) {
            if (!(obj instanceof String))
                throw new IllegalArgumentException("Cannot dereference a "+obj.getClass());
            if (isValidUrl((String)obj)) {
                /*
                    This is a fully qualified URL "https://somesite.com/data/cities.csv".
                 */
                dereferencedObj.add(new URL((String)obj));
            } else {
                if (basePath instanceof Path) {
                    /*
                        relative path, store for later dereferencing.
                        For reading, must be read relative to the basePath
                     */
                    dereferencedObj.add(new File((String) obj));
                } else if (basePath instanceof URL) {
                    /*
                        This is a URL fragment "data/cities.csv".
                        According to https://github.com/frictionlessdata/specs/issues/652,
                        it should be parsed against the base URL (the Descriptor URL)
                     */
                    dereferencedObj.add(new URL(((URL)basePath),(String)obj));
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
}