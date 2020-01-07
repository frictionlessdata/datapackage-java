package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.exceptions.DataPackageFileOrUrlNotFoundException;
import io.frictionlessdata.datapackage.resource.Resource;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.datasourceformats.DataSourceFormat;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static io.frictionlessdata.datapackage.Package.isValidUrl;

public abstract class JSONBase {
    static final int JSON_INDENT_FACTOR = 4;// JSON keys.
    // FIXME: Use somethign like GSON instead so this explicit mapping is not
    // necessary?
    public static final String JSON_KEY_NAME = "name";
    public static final String JSON_KEY_PROFILE = "profile";
    public static final String JSON_KEY_PATH = "path";
    public static final String JSON_KEY_DATA = "data";
    public static final String JSON_KEY_TITLE = "title";
    public static final String JSON_KEY_DESCRIPTION = "description";
    public static final String JSON_KEY_FORMAT = "format";
    public static final String JSON_KEY_MEDIA_TYPE = "mediaType";
    public static final String JSON_KEY_ENCODING = "encoding";
    public static final String JSON_KEY_BYTES = "bytes";
    public static final String JSON_KEY_HASH = "hash";
    public static final String JSON_KEY_SCHEMA = "schema";
    public static final String JSON_KEY_DIALECT = "dialect";
    public static final String JSON_KEY_SOURCES = "sources";
    public static final String JSON_KEY_LICENSES = "licenses";

    /**
     * If true, we are reading from an archive format, eg. ZIP
     */
    boolean isArchivePackage = false;
    // Metadata properties.
    // Required properties.
    private String name;

    // Recommended properties.
    private String profile = null;

    // Optional properties.
    private String title = null;
    private String description = null;


    String format = null;
    private String mediaType = null;
    private String encoding = null;
    private Integer bytes = null;
    private String hash = null;

    Dialect dialect;
    private JSONArray sources = null;
    private JSONArray licenses = null;

    // Schema
    private Schema schema = null;

    protected Map<String, Object> originalReferences = new HashMap<>();
    /**
     * @return the name
     */
    public String getName(){return name;}

    /**
     * @param name the name to set
     */
    public void setName(String name) {this.name = name;}

    /**
     * @return the profile
     */
    public String getProfile(){return profile;}

    /**
     * @param profile the profile to set
     */
    public void setProfile(String profile){this.profile = profile;}

    /**
     * @return the title
     */
    public String getTitle(){return title;}

    /**
     * @param title the title to set
     */
    public void setTitle(String title){this.title = title;}

    /**
     * @return the description
     */
    public String getDescription(){return description;}

    /**
     * @param description the description to set
     */
    public void setDescription(String description){this.description = description;}


    /**
     * @return the mediaType
     */
    public String getMediaType(){return mediaType;}

    /**
     * @param mediaType the mediaType to set
     */
    public void setMediaType(String mediaType){this.mediaType = mediaType;}

    /**
     * @return the encoding
     */
    public String getEncoding(){return encoding;}

    /**
     * @param encoding the encoding to set
     */
    public void setEncoding(String encoding){this.encoding = encoding;}

    /**
     * @return the bytes
     */
    public Integer getBytes(){return bytes;}

    /**
     * @param bytes the bytes to set
     */
    public void setBytes(Integer bytes){this.bytes = bytes;}

    /**
     * @return the hash
     */
    public String getHash(){return  hash;}

    /**
     * @param hash the hash to set
     */
    public void setHash(String hash){this.hash = hash;}

    public Schema getSchema(){return schema;}

    public void setSchema(Schema schema){this.schema = schema;}

    public String getSchemaReference() {
        if (null == originalReferences.get(JSONBase.JSON_KEY_SCHEMA))
            return null;
        return originalReferences.get(JSONBase.JSON_KEY_SCHEMA).toString();
    }

    public JSONArray getSources(){
        return sources;
    }

    public void setSources(JSONArray sources){
        this.sources = sources;
    }
    /**
     * @return the licenses
     */
    public JSONArray getLicenses(){return  licenses;}

    /**
     * @param licenses the licenses to set
     */
    public void setLicenses(JSONArray licenses){this.licenses = licenses;}


    public static Schema buildSchema(JSONObject resourceJson, Object basePath, boolean isArchivePackage) throws Exception {
        // Get the schema and dereference it. Enables validation against it.
        Object schemaObj = resourceJson.has(JSONBase.JSON_KEY_SCHEMA) ? resourceJson.get(JSONBase.JSON_KEY_SCHEMA) : null;
        JSONObject dereferencedSchema = dereference(schemaObj, basePath, isArchivePackage);
        if (null != dereferencedSchema) {
            return Schema.fromJson(dereferencedSchema.toString(), false);
        }
        return null;
    }

    public static Dialect buildDialect (JSONObject resourceJson, Object basePath, boolean isArchivePackage) throws Exception {
        // Get the dialect and dereference it. Enables validation against it.
        Object dialectObj = resourceJson.has(JSONBase.JSON_KEY_DIALECT) ? resourceJson.get(JSONBase.JSON_KEY_DIALECT) : null;
        JSONObject dereferencedDialect = dereference(dialectObj, basePath, isArchivePackage);
        if (null != dereferencedDialect) {
            return Dialect.fromJson(dereferencedDialect.toString());
        }
        return null;
    }

    public static void setFromJson(JSONObject resourceJson, JSONBase retVal, Schema schema) {
        if (resourceJson.has(JSONBase.JSON_KEY_SCHEMA))
            retVal.originalReferences.put(JSONBase.JSON_KEY_SCHEMA, resourceJson.get(JSONBase.JSON_KEY_SCHEMA));
        if (resourceJson.has(JSONBase.JSON_KEY_DIALECT))
            retVal.originalReferences.put(JSONBase.JSON_KEY_DIALECT, resourceJson.get(JSONBase.JSON_KEY_DIALECT));

        //FIXME: Again, could be greatly simplified amd much more
        // elegant if we use a library like GJSON...
        String name = resourceJson.has(JSONBase.JSON_KEY_NAME) ? resourceJson.getString(JSONBase.JSON_KEY_NAME) : null;
        String profile = resourceJson.has(JSONBase.JSON_KEY_PROFILE) ? resourceJson.getString(JSONBase.JSON_KEY_PROFILE) : null;
        String title = resourceJson.has(JSONBase.JSON_KEY_TITLE) ? resourceJson.getString(JSONBase.JSON_KEY_TITLE) : null;
        String description = resourceJson.has(JSONBase.JSON_KEY_DESCRIPTION) ? resourceJson.getString(JSONBase.JSON_KEY_DESCRIPTION) : null;
        String mediaType = resourceJson.has(JSONBase.JSON_KEY_MEDIA_TYPE) ? resourceJson.getString(JSONBase.JSON_KEY_MEDIA_TYPE) : null;
        String encoding = resourceJson.has(JSONBase.JSON_KEY_ENCODING) ? resourceJson.getString(JSONBase.JSON_KEY_ENCODING) : null;
        Integer bytes = resourceJson.has(JSONBase.JSON_KEY_BYTES) ? resourceJson.getInt(JSONBase.JSON_KEY_BYTES) : null;
        String hash = resourceJson.has(JSONBase.JSON_KEY_HASH) ? resourceJson.getString(JSONBase.JSON_KEY_HASH) : null;

        JSONArray sources = resourceJson.has(JSONBase.JSON_KEY_SOURCES) ? resourceJson.getJSONArray(JSONBase.JSON_KEY_SOURCES) : null;
        JSONArray licenses = resourceJson.has(JSONBase.JSON_KEY_LICENSES) ? resourceJson.getJSONArray(JSONBase.JSON_KEY_LICENSES) : null;

        retVal.setName(name);
        retVal.setSchema(schema);
        retVal.setProfile(profile);
        retVal.setTitle(title);
        retVal.setDescription(description);
        retVal.setMediaType(mediaType);
        retVal.setEncoding(encoding);
        retVal.setBytes(bytes);
        retVal.setHash(hash);
        retVal.setSources(sources);
        retVal.setLicenses(licenses);
    }


    static String getFileContentAsString(InputStream stream) {
        try (BufferedReader rdr = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            List<String> lines = rdr
                    .lines()
                    .collect(Collectors.toList());
            return String.join("\n", lines);
        }  catch (Exception ex) {
            throw new DataPackageException(ex);
        }
    }

    static String getFileContentAsString(URL url) {
        try {
            return getFileContentAsString(url.openStream());
        } catch (Exception ex) {
            throw new DataPackageException(ex);
        }
    }

    static String getFileContentAsString(File file) {
        try (InputStream is = new FileInputStream(file)){
            return getFileContentAsString(is);
        } catch (Exception ex) {

            if ((ex instanceof NoSuchFileException
                    || (ex instanceof FileNotFoundException))) {
                throw new DataPackageFileOrUrlNotFoundException(ex);
            }
            throw new DataPackageException(ex);
        }
    }

    static String getFileContentAsString(Path filePath) {
        try (InputStream is = Files.newInputStream(filePath)){
            return getFileContentAsString(is);
        } catch (Exception ex) {
            if ((ex instanceof NoSuchFileException
            || (ex instanceof FileNotFoundException))) {
                throw new DataPackageFileOrUrlNotFoundException(ex);
            }
            throw new DataPackageException(ex);
        }
    }

    /**
     * Take a ZipFile and look for the `filename` entry. If it is not on the top-level,
     * look for directories and go into them (but only one level deep) and look again
     * for the `filename` entry
     * @param zipFile the ZipFile to use for looking for the `filename` entry
     * @param fileName name of the entry we are looking for
     * @return ZipEntry if found, null otherwise
     */
    private static ZipEntry findZipEntry(ZipFile zipFile, String fileName) {
        ZipEntry entry = zipFile.getEntry(fileName);
        if (null != entry)
            return entry;
        else {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry zipEntry = entries.nextElement();
                if (zipEntry.isDirectory()) {
                    entry = zipFile.getEntry(zipEntry.getName()+fileName);
                    if (null != entry)
                        return entry;
                }
            }
        }
        return null;
    }

    protected static String getZipFileContentAsString(Path inFilePath, String fileName) throws IOException {
        // Read in memory the file inside the zip.
        ZipFile zipFile = new ZipFile(inFilePath.toFile());
        ZipEntry entry = findZipEntry(zipFile, fileName);

        // Throw exception if expected datapackage.json file not found.
        if(entry == null){
            throw new DataPackageException("The zip file does not contain the expected file: " + fileName);
        }

        // Read the datapackage.json file inside the zip
        try (InputStream stream = zipFile.getInputStream(entry)) {
            return getFileContentAsString(stream);
        }
    }

    public static JSONObject dereference(File fileObj, Path basePath, boolean isArchivePackage) throws IOException {
        String jsonContentString;
        if (isArchivePackage) {
            String filePath = fileObj.getPath();
            if (File.separator.equals("\\")) {
                filePath = filePath.replaceAll("\\\\", "/");
            }
            jsonContentString = getZipFileContentAsString(basePath, filePath);
        } else {
            /* If reference is file path.
               from the spec: "SECURITY: / (absolute path) and ../ (relative parent path)
               are forbidden to avoid security vulnerabilities when implementing data
               package software."

               https://frictionlessdata.io/specs/data-resource/index.html#url-or-path
             */
            Path securePath = Resource.toSecure(fileObj.toPath(), basePath);
            if (securePath.toFile().exists()) {
                // Create the dereferenced schema object from the local file.
                jsonContentString = getFileContentAsString(securePath.toFile());
            } else {
                throw new DataPackageFileOrUrlNotFoundException("Local file not found: " + fileObj);
            }
        }
        return new JSONObject(jsonContentString);
    }

    /**
     * Take a string containing either a fully qualified URL or a path-fragment and read the contents
     * of either the fully qualified URL or the basePath URL+path-fragment. Construct a JSON object
     * from the URL content
     * @param url fully qualified URL or path fragment
     * @param basePath base URL, only used if we are dealing with a path fragment
     * @return a JSONObject built from the url content
     * @throws IOException if fetching the contents of the URL goes wrong
     */

    private static JSONObject dereference(String url, URL basePath) throws IOException {
        JSONObject dereferencedObj = null;

        if (isValidUrl(url)) {
            // Create the dereferenced object from the remote file.
            String jsonContentString = getFileContentAsString(new URL(url));
            dereferencedObj = new JSONObject(jsonContentString);
        } else {
            URL lURL = new URL(basePath.toExternalForm()+url);
            if (isValidUrl(lURL)) {
                String jsonContentString = getFileContentAsString(lURL);
                dereferencedObj = new JSONObject(jsonContentString);
            } else {
                throw new DataPackageFileOrUrlNotFoundException("URL not found"+lURL);
            }
        }
        return dereferencedObj;
    }

    public static JSONObject dereference(Object obj, Object basePath, boolean isArchivePackage) throws IOException {
        if (null == obj)
            return null;
        // Object is already a dereferenced object.
        if(obj instanceof JSONObject){
            // Don't need to do anything, just cast and return.
            return (JSONObject)obj;
        } else if(obj instanceof String){
            String reference = (String)obj;
            if (isValidUrl(reference))
                if (basePath instanceof File) {
                    String jsonString = getFileContentAsString(new URL(reference));
                    return new JSONObject(jsonString);
                }
                else {
                    String jsonString = getFileContentAsString(new URL(reference));
                    return new JSONObject(jsonString);
                }
            else if (basePath instanceof URL) {
                return dereference(reference, (URL) basePath);
            } else
                return dereference(new File(reference), (Path)basePath, isArchivePackage);
        }

        return null;
    }
}
