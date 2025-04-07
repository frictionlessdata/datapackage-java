package io.frictionlessdata.datapackage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.exceptions.DataPackageFileOrUrlNotFoundException;
import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.datapackage.resource.Resource;
import io.frictionlessdata.tableschema.exception.JsonParsingException;
import io.frictionlessdata.tableschema.io.FileReference;
import io.frictionlessdata.tableschema.io.LocalFileReference;
import io.frictionlessdata.tableschema.io.URLFileReference;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.util.JsonUtil;

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

import static io.frictionlessdata.datapackage.Validator.isValidUrl;

@JsonInclude(value = Include.NON_EMPTY, content = Include.NON_EMPTY )
public abstract class JSONBase implements BaseInterface {
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
    @JsonIgnore
    boolean isArchivePackage = false;
    // Metadata properties.
    // Required properties.
    protected String name;

    // Recommended properties.
    protected String profile = null;

    // Optional properties.
    protected String title = null;
    protected String description = null;

    protected String mediaType = null;
    protected String encoding = null;
    protected Integer bytes = null;
    protected String hash = null;

    private List<Source> sources = null;
    private List<License> licenses = null;

    @JsonIgnore
    protected Map<String, String> originalReferences = new HashMap<>();
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

    public List<Source> getSources(){
        return sources;
    }

    public void setSources(List<Source> sources){
        this.sources = sources;
    }
    /**
     * @return the licenses
     */
    public List<License> getLicenses(){return  licenses;}

    /**
     * @param licenses the licenses to set
     */
    public void setLicenses(List<License> licenses){this.licenses = licenses;}

    @JsonIgnore
    public Map<String, String> getOriginalReferences() {
        return originalReferences;
    }

    public static Schema buildSchema(JsonNode resourceJson, Object basePath, boolean isArchivePackage)
            throws Exception {
        FileReference ref = referenceFromJson(resourceJson, JSON_KEY_SCHEMA, basePath);
        if (null != ref) {
            return Schema.fromJson(ref, true);
        }
        Object schemaObj = resourceJson.has(JSON_KEY_SCHEMA)
                ? resourceJson.get(JSON_KEY_SCHEMA)
                : null;
        if (null == schemaObj)
            return null;
        return Schema.fromJson(dereference(schemaObj, basePath, isArchivePackage).toString(), true);
    }

    public static Dialect buildDialect (JsonNode resourceJson, Object basePath, boolean isArchivePackage)
            throws Exception {
        FileReference ref = referenceFromJson(resourceJson, JSON_KEY_DIALECT, basePath);
        if (null != ref) {
            return Dialect.fromJson(ref);
        }
        Object dialectObj = resourceJson.has(JSON_KEY_DIALECT)
                ? resourceJson.get(JSON_KEY_DIALECT)
                : null;
        if (null == dialectObj)
            return null;
        return Dialect.fromJson(dereference(dialectObj, basePath, isArchivePackage).toString());
    }

    private static FileReference referenceFromJson(JsonNode resourceJson, String key, Object basePath)
            throws IOException {
        Object dialectObj = resourceJson.has(key)
                ? resourceJson.get(key)
                : null;
        if (null == dialectObj)
            return null;
        Object refObj = determineType(dialectObj, basePath);
        FileReference ref = null;
        if (refObj instanceof URL) {
            ref = new URLFileReference((URL)refObj);
        } else if (refObj instanceof File) {
            ref = new LocalFileReference(((Path)basePath).toFile(), dialectObj.toString());
        }
        return ref;
    }

    public static void setFromJson(JsonNode resourceJson, JSONBase retVal) {
        if (resourceJson.has(JSONBase.JSON_KEY_SCHEMA) && resourceJson.get(JSONBase.JSON_KEY_SCHEMA).isTextual())
            retVal.originalReferences.put(JSONBase.JSON_KEY_SCHEMA, resourceJson.get(JSONBase.JSON_KEY_SCHEMA).asText());
        if (resourceJson.has(JSONBase.JSON_KEY_DIALECT) && resourceJson.get(JSONBase.JSON_KEY_DIALECT).isTextual())
            retVal.originalReferences.put(JSONBase.JSON_KEY_DIALECT, resourceJson.get(JSONBase.JSON_KEY_DIALECT).asText());

        // TODO: A mapper library might be useful, but not required
        String name = textValueOrNull(resourceJson, JSONBase.JSON_KEY_NAME);
        String profile = textValueOrNull(resourceJson, JSONBase.JSON_KEY_PROFILE);
        String title = textValueOrNull(resourceJson, JSONBase.JSON_KEY_TITLE);
        String description = textValueOrNull(resourceJson, JSONBase.JSON_KEY_DESCRIPTION);
        String mediaType = textValueOrNull(resourceJson, JSONBase.JSON_KEY_MEDIA_TYPE);
        String encoding = textValueOrNull(resourceJson, JSONBase.JSON_KEY_ENCODING);
        Integer bytes = resourceJson.has(JSONBase.JSON_KEY_BYTES) ? resourceJson.get(JSONBase.JSON_KEY_BYTES).asInt() : null;
        String hash = textValueOrNull(resourceJson, JSONBase.JSON_KEY_HASH);

        List<Source> sources = null;
        if(resourceJson.has(JSONBase.JSON_KEY_SOURCES) && resourceJson.get(JSON_KEY_SOURCES).isArray()) {
        	sources = JsonUtil.getInstance().deserialize(resourceJson.get(JSONBase.JSON_KEY_SOURCES), new TypeReference<>() {});
        }
        List<License> licenses = null;
        if(resourceJson.has(JSONBase.JSON_KEY_LICENSES) && resourceJson.get(JSONBase.JSON_KEY_LICENSES).isArray()){
            licenses = JsonUtil.getInstance().deserialize(resourceJson.get(JSONBase.JSON_KEY_LICENSES), new TypeReference<>() {});
        }

        retVal.setName(name);
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

    private static String textValueOrNull(JsonNode source, String fieldName) {
    	return source.has(fieldName) ? source.get(fieldName).asText() : null;
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
            if (ex instanceof FileNotFoundException)
                throw new DataPackageValidationException(ex.getMessage(), ex);
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

        String content;
        // Read the datapackage.json file inside the zip
        try (InputStream stream = zipFile.getInputStream(entry)) {
            content = getFileContentAsString(stream);
        }
        zipFile.close();
        return content;
    }

    protected static byte[] getZipFileContentAsByteArray(Path inFilePath, String fileName) throws IOException {
        // Read in memory the file inside the zip.
        ZipFile zipFile = new ZipFile(inFilePath.toFile());
        ZipEntry entry = findZipEntry(zipFile, fileName);

        // Throw exception if expected datapackage.json file not found.
        if(entry == null){
            throw new DataPackageException("The zip file does not contain the expected file: " + fileName);
        }
        try (InputStream inputStream = zipFile.getInputStream(entry);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            for (int b; (b = inputStream.read()) != -1; ) {
                out.write(b);
            }
            return out.toByteArray();
        }
    }

    public static ObjectNode dereference(File fileObj, Path basePath, boolean isArchivePackage) throws IOException {
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
        return (ObjectNode) createNode(jsonContentString);
    }

    /**
     * Take a string containing either a fully qualified URL or a path-fragment and read the contents
     * of either the fully qualified URL or the basePath URL+path-fragment. Construct a JSON object
     * from the URL content
     * @param url fully qualified URL or path fragment
     * @param basePath base URL, only used if we are dealing with a path fragment
     * @return a JsonNode built from the url content
     * @throws IOException if fetching the contents of the URL goes wrong
     */

    private static ObjectNode dereference(String url, URL basePath) throws IOException {
        JsonNode dereferencedObj;

        if (isValidUrl(url)) {
            // Create the dereferenced object from the remote file.
            String jsonContentString = getFileContentAsString(new URL(url));
            dereferencedObj = createNode(jsonContentString);
        } else {
            URL lURL = new URL(basePath.toExternalForm()+url);
            if (isValidUrl(lURL)) {
                String jsonContentString = getFileContentAsString(lURL);
                dereferencedObj = createNode(jsonContentString);
            } else {
                throw new DataPackageFileOrUrlNotFoundException("URL not found"+lURL);
            }
        }
        return (ObjectNode) dereferencedObj;
    }

    public static ObjectNode dereference(Object obj, Object basePath, boolean isArchivePackage) throws IOException {
        if (null == obj)
            return null;
        // Object is already a dereferenced object.
        else if(obj instanceof ObjectNode){
            // Don't need to do anything, just cast and return.
            return (ObjectNode)obj;
        } else if (obj instanceof TextNode) {
        	return dereference(((TextNode) obj).asText(), basePath, isArchivePackage);
        } else if(obj instanceof String){
            String reference = (String)obj;
            if (isValidUrl(reference))
                if (basePath instanceof File) {
                    String jsonString = getFileContentAsString(new URL(reference));
                    return (ObjectNode) createNode(jsonString);
                }
                else {
                    String jsonString = getFileContentAsString(new URL(reference));
                    return (ObjectNode) createNode(jsonString);
                }
            else if (basePath instanceof URL) {
                return dereference(reference, (URL) basePath);
            } else
                return dereference(new File(reference), (Path)basePath, isArchivePackage);
        }

        return null;
    }

    protected static JsonNode createNode(String json) {
    	try {
    		return JsonUtil.getInstance().createNode(json);
    	} catch (JsonParsingException ex) {
        	throw new DataPackageException(ex);
        }
    }

    public static Object determineType(Object obj, Object basePath) throws IOException {
        if (null == obj)
            return null;
        // Object is already a dereferenced object.
        if(obj instanceof JsonNode){
            // Don't need to do anything, just cast and return.
            return obj;
        } else if(obj instanceof String){
            String reference = (String)obj;
            if (isValidUrl(reference)){
                return new URL(reference);
            }
            else if (basePath instanceof URL) {
                return new URL(((URL)basePath), reference);
            } else {
                return new File(((Path)basePath).toFile(), reference);
            }
        }

        return null;
    }
}
