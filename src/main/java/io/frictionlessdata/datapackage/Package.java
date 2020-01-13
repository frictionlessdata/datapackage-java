package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.resource.*;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.Table;
import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.collections.set.UnmodifiableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.everit.json.schema.ValidationException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.*;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;


/**
 * Load, validate, create, and save a datapackage object according to the specifications at
 * https://github.com/frictionlessdata/specs/blob/master/specs/data-package.md
 */
public class Package extends JSONBase{
    public static final String DATAPACKAGE_FILENAME = "datapackage.json";
    private static final String JSON_KEY_RESOURCES = "resources";
    private static final String JSON_KEY_ID = "id";
    private static final String JSON_KEY_VERSION = "version";
    private static final String JSON_KEY_HOMEPAGE = "homepage";
    private static final String JSON_KEY_KEYWORDS = "keywords";
    private static final String JSON_KEY_IMAGE = "image";
    private static final String JSON_KEY_CREATED = "created";
    private static final String JSON_KEY_CONTRIBUTORS = "contributors";

    private Object basePath = null;
    private String id;
    private String version;
    private int[] versionParts;
    private URL homepage;
    private Set<String> keywords = new TreeSet<>();
    private String image;
    private ZonedDateTime created;
    private List<Contributor> contributors = new ArrayList<>();
    private Map<String, Object> otherProperties = new LinkedHashMap<>();
    
    private JSONObject jsonObject = new JSONObject();
    private boolean strictValidation = false;
    private List<Resource> resources = new ArrayList<>();
    private List<Exception> errors = new ArrayList<>();
    private Validator validator = new Validator();

    /**
     * Create a new DataPackage and initialize with a number of Resources.
     *
     * According to the spec, a DataPackage MUST have at least one Resource,
     * therefore an Exception will be thrown if `resources` is empty.
     *
     * @param resources the initial Resouces for the DataPackage
     */
    public Package(Collection<Resource> resources) throws IOException {
        for (Resource r : resources) {
            addResource(r);
                   }
        UUID uuid = UUID.randomUUID();
        id = uuid.toString();
        validate();
    }
    
    /**
     * Load from String representation of JSON object. To prevent file system traversal attacks,
     * the basePath must be explicitly set here, the `basePath` variable cannot be null.
     *
     * The basePath is the path that is used as a jail root for Resource creation -
     * no absolute paths for Resources are allowed, they must all be relative to and
     * children of the basePath.
     *
     * Security: JSON-based Packages can load Resources as JSON strings, from
     * local files below the basePath or from URLs.
     *
     * @param jsonStringSource the String representation of the Descriptor JSON object
     * @param strict whether to use strict schema parsing
     * @throws IOException thrown if I/O operations fail
     * @throws DataPackageException thrown if constructing the Descriptor fails
     */
    public Package(String jsonStringSource, Path basePath, boolean strict) throws Exception {
        this.strictValidation = strict;
        if (null == basePath)
            throw new DataPackageException("basePath cannot be null for JSON-based DataPackages ");
        this.basePath = basePath;

        // Create and set the JSONObject fpr the String representation of desriptor JSON object.
        this.setJson(new JSONObject(jsonStringSource));

        // If String representation of desriptor JSON object is provided.
        this.validate();
    }

    /**
     * Load from URL (must be in either 'http' or 'https' schemes).
     *
     * From the specification: "URLs MUST be fully qualified. MUST be using either
     * http or https scheme." (https://frictionlessdata.io/specs/data-resource/#url-or-path)
     *
     * Security: URL-based Packages can only load Resources as JSON strings or from other URLs,
     * not from the local file system.
     *
     * @param urlSource The URL that points to the DataPackage Descriptor (if it's in
     *                  a directory on the server) or the ZIP file if it's a ZIP-based
     *                  package.
     * @param strict whether to use strict schema parsing
     *
     * @throws IOException thrown if I/O operations fail
     * @throws DataPackageException thrown if constructing the Descriptor fails
     */
    public Package(URL urlSource, boolean strict) throws Exception {
        this.strictValidation = strict;
        this.basePath = getParentUrl(urlSource);

        if (!isValidUrl(urlSource.toExternalForm())) {
            throw new DataPackageException("URL form not valid: "+urlSource.toExternalForm());
        }
        // Get string content of given remove file.
        String jsonString = getFileContentAsString(urlSource);

        // Create JSONObject and validate.
        this.setJson(new JSONObject(jsonString));
        this.validate();  
        
    }

    /**
     * Load from local file path. The file path to the descriptor can be either absolute
     * or relative to the working directory. However, to prevent file system traversal attacks,
     * we explicitly set the basePath here to the parent directory of the Descriptor file.
     * The basePath is the path that is used as a jail root for Resource creation -
     * no absolute paths for Resources are allowed, they must all be relative to and
     * children of the basePath.
     *
     * From the specification: "POSIX paths (unix-style with / as separator) are supported
     * for referencing local files, with the security restraint that they MUST be relative
     * siblings or children of the descriptor.
     * Absolute paths (/) and relative parent paths (../) MUST NOT be used, and
     * implementations SHOULD NOT support these path types."
     * (https://frictionlessdata.io/specs/data-resource/#url-or-path)
     *
     * Security: local file path-based Packages can load Resources as JSON strings, from
     * local files below the basePath or from URLs
     *
     * @param descriptorFile local file path that points to the DataPackage Descriptor (if it's in
     *                  a local directory) or the ZIP file if it's a ZIP-based
     *                  package.
     * @param strict whether to use strict schema parsing
     * @throws IOException thrown if I/O operations fail
     * @throws DataPackageException thrown if constructing the Descriptor fails
     */
    public Package(Path descriptorFile, boolean strict) throws Exception {
        this.strictValidation = strict;
        JSONObject sourceJsonObject;
        if (descriptorFile.toFile().getName().toLowerCase().endsWith(".zip")) {
            isArchivePackage = true;
            basePath = descriptorFile;
            sourceJsonObject = new JSONObject(JSONBase.getZipFileContentAsString(descriptorFile, DATAPACKAGE_FILENAME));
        } else {
            basePath = descriptorFile.getParent();
            String sourceJsonString = getFileContentAsString(descriptorFile);
            sourceJsonObject = new JSONObject(sourceJsonString);
        }
        this.setJson(sourceJsonObject);
        this.validate();
    }


    private FileSystem getTargetFileSystem(File outputDir, boolean zipCompressed) throws IOException {
        FileSystem outFs = null;
        if (zipCompressed) {
            if (outputDir.exists()) {
                throw new DataPackageException("Cannot save into existing ZIP file: "
                        +outputDir.getName());
            }
            Map<String, String> env = new HashMap<>();
            env.put("create", "true");
            outFs = FileSystems.newFileSystem(URI.create("jar:" + outputDir.toURI().toString()), env);
        } else {
            if (!(outputDir.isDirectory())) {
                throw new DataPackageException("Target for save() exists and is a regular file: "
                        +outputDir.getName());
            }
            outFs = outputDir.toPath().getFileSystem();
        }
        return outFs;
    }

    private void writeDescriptor (FileSystem outFs, String parentDirName) throws IOException {
        Path nf = outFs.getPath(parentDirName+File.separator+DATAPACKAGE_FILENAME);
        try (Writer writer = Files.newBufferedWriter(nf, StandardCharsets.UTF_8, StandardOpenOption.CREATE)) {
            writer.write(this.getJsonObject().toString(JSON_INDENT_FACTOR));
        }
    }

    /**
     * Convert all Resources to CSV files, no matter whether they come from
     * URLs, JSON Arrays, or files originally. The result is just one JSON
     * file
     * @param outputDir the directory or ZIP file to write the "datapackage.json"
     *                  file to
     * @param zipCompressed whether we are writing to a ZIP archive
     * @throws Exception thrown if something goes wrong writing
     */
    public void writeFullyInlined (File outputDir, boolean zipCompressed) throws Exception {
        FileSystem outFs = getTargetFileSystem(outputDir, zipCompressed);
        String parentDirName = "";
        if (!zipCompressed) {
            parentDirName = outputDir.getPath();
        }
        writeDescriptor(outFs, parentDirName);

        for (Resource r : this.resources) {
            r.writeDataAsCsv(outFs.getPath(parentDirName+File.separator), dialect);
        }
    }

    /**
     * Write this datapackage to an output directory or ZIP file. Creates at least a
     * datapackage.json file and if this Package object holds file-based
     * resources, dialect, or schemas, creates them as files.
     * @param outputDir the directory or ZIP file to write the files to
     * @param zipCompressed whether we are writing to a ZIP archive
     * @throws Exception thrown if something goes wrong writing
     */
    public void write (File outputDir, boolean zipCompressed) throws Exception {
        System.out.println("#"+outputDir.toString());
        FileSystem outFs = getTargetFileSystem(outputDir, zipCompressed);
        System.out.println("##"+outFs.toString());
        String parentDirName = "";
        if (!zipCompressed) {
            parentDirName = outputDir.getPath();
        }
        System.out.println("###"+parentDirName.toString());
        writeDescriptor(outFs, parentDirName);

        // only file-based Resources need to be written to the DataPackage, URLs stay as
        // external references and JSONArray-based Resources got serialized as part of the
        // Descriptor file
        final List<Resource> resourceList = resources
                .stream()
                .filter(Resource::shouldSerializeToFile)
                .collect(Collectors.toList());

        for (Resource r : resourceList) {
            System.out.println("####"+outFs.getPath(parentDirName).toString());
            r.writeDataAsCsv(outFs.getPath(parentDirName), dialect);
            String schemaRef = r.getSchemaReference();
            // write out schema file only if not null or URL
            if ((null != schemaRef) && (!isValidUrl(schemaRef))) {
                // URL fragments will not be written to disk either
                if (!(r instanceof URLbasedResource)) {
                    Path schemaP = outFs.getPath(parentDirName+File.separator+schemaRef);
                    writeSchema(schemaP, r.getSchema());
                }
            }
            String dialectRef = r.getDialectReference();
            // write out schema file only if not null or URL
            if ((null != dialectRef) && (!isValidUrl(dialectRef))) {
                // URL fragments will not be written to disk either
                if (!(r instanceof URLbasedResource)) {
                    Path dialectP = outFs.getPath(parentDirName+File.separator+dialectRef);
                    writeDialect(dialectP, r.getDialect());
                }
            }
        }
        // ZIP-FS needs close, but WindowsFileSystem unsurprisingly doesn't
        // like to get closed...
        try {
            outFs.close();
        } catch (UnsupportedOperationException es) {};
    }

    public void writeJson (File outputFile) throws IOException{
        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            writeJson(fos);
        }
    }

    public void writeJson (OutputStream output) throws IOException{
        try (BufferedWriter file = new BufferedWriter(new OutputStreamWriter(output))) {
            file.write(this.getJsonObject().toString(JSON_INDENT_FACTOR));
        }
    }

    private void saveZip(File outputFilePath) throws IOException, DataPackageException{
        try(FileOutputStream fos = new FileOutputStream(outputFilePath)){
            try(BufferedOutputStream bos = new BufferedOutputStream(fos)){
                try(ZipOutputStream zos = new ZipOutputStream(bos)){
                    // File is not on the disk, test.txt indicates
                    // only the file name to be put into the zip.
                    ZipEntry entry = new ZipEntry(DATAPACKAGE_FILENAME);

                    zos.putNextEntry(entry);
                    zos.write(this.getJsonObject().toString(JSON_INDENT_FACTOR).getBytes());
                    zos.closeEntry();
                }
            }
        }
    }

    // TODO migrate into Schema.java
    private static void writeSchema(Path parentFilePath, Schema schema) throws IOException {
        if (!Files.exists(parentFilePath)) {
            Files.createDirectories(parentFilePath);
        }
        Files.deleteIfExists(parentFilePath);
        try (Writer wr = Files.newBufferedWriter(parentFilePath, StandardCharsets.UTF_8)) {
            wr.write(schema.getJson());
        }
    }


    // TODO migrate into Dialet.java
    private static void writeDialect(Path parentFilePath, Dialect dialect) throws IOException {
        if (!Files.exists(parentFilePath)) {
            Files.createDirectories(parentFilePath);
        }
        Files.deleteIfExists(parentFilePath);
        try (Writer wr = Files.newBufferedWriter(parentFilePath, StandardCharsets.UTF_8)) {
            wr.write(dialect.getJson());
        }
    }
    
    public Resource getResource(String resourceName){
        for (Resource resource : this.resources) {
            if (resource.getName().equalsIgnoreCase(resourceName)) {
                return resource;
            }
        }
        return null;
    }

    
    public List<Resource> getResources(){
        return this.resources;
    }

    private void validate(DataPackageException dpe) throws IOException {
        if (dpe != null) {
            if (this.strictValidation) {
                throw dpe;
            } else {
                errors.add(dpe);
            }
        }

        // Validate.
        this.validate();
    }

    private DataPackageException checkDuplicates(Resource resource) {
        DataPackageException dpe = null;
        // Check if there is duplication.
        for (Resource value : this.resources) {
            if (value.getName().equalsIgnoreCase(resource.getName())) {
                dpe = new DataPackageException(
                        "A resource with the same name already exists.");
            }
        }
        return dpe;
    }

    public void addResource(Resource resource)
            throws IOException, ValidationException, DataPackageException{
        DataPackageException dpe = null;
        if (resource.getName() == null){
            dpe = new DataPackageException("Invalid Resource, it does not have a name property.");
        }
        if (resource instanceof AbstractDataResource)
            addResource((AbstractDataResource) resource);
        else if (resource instanceof AbstractReferencebasedResource)
            addResource((AbstractReferencebasedResource) resource);
        validate(dpe);
    }

    void addResource(AbstractDataResource resource)
            throws IOException, ValidationException, DataPackageException{
        DataPackageException dpe = null;
        // If a name property isn't given...
        if ((resource).getData() == null || (resource).getFormat() == null) {
                dpe = new DataPackageException("Invalid Resource. The data and format properties cannot be null.");
        } else {
            dpe = checkDuplicates(resource);
        }
        validate(dpe);

        this.resources.add(resource);
    }

    void addResource(AbstractReferencebasedResource resource)
            throws IOException, ValidationException, DataPackageException{
        DataPackageException dpe = null;
        if (resource.getPaths() == null) {
                dpe = new DataPackageException("Invalid Resource. The path property cannot be null.");
        } else {
            dpe = checkDuplicates(resource);
        }
        validate(dpe);

        this.resources.add(resource);
    }

    void removeResource(String name){
        this.resources.removeIf(resource -> resource.getName().equalsIgnoreCase(name));
    }

    
    void addProperty(String key, String value) throws DataPackageException{
        if(this.getJsonObject().has(key)){
            throw new DataPackageException("A property with the same key already exists.");
        }else{
            this.getJsonObject().put(key, value);
        }
    }
    
    public void addProperty(String key, JSONObject value) throws DataPackageException{
        if(this.getJsonObject().has(key)){
            throw new DataPackageException("A property with the same key already exists.");
        }else{
            this.getJsonObject().put(key, value);
        }
    }
    
    public void addProperty(String key, JSONArray value) throws DataPackageException{
        if(this.getJsonObject().has(key)){
            throw new DataPackageException("A property with the same key already exists.");
        }else{
            this.getJsonObject().put(key, value);
        }
    }
    
    public void removeProperty(String key){
        this.getJsonObject().remove(key);
    }
    
    /**
     * Validation is strict or unstrict depending on how the package was
     * instantiated with the strict flag.
     * @throws IOException
     * @throws DataPackageException
     */
    final void validate() throws IOException, DataPackageException{
        try{
            this.validator.validate(this.getJson());
        }catch(ValidationException ve){
            if(this.strictValidation){
                throw ve;
            }else{
                errors.add(ve);
            }
        }
    }
    
    final Path getBasePath(){
        if (basePath instanceof File)
            return ((File)this.basePath).toPath();
        else if (basePath instanceof Path)
            return (Path) basePath;
        else
            return null;
    }


    final URL getBaseUrl(){
        if (basePath instanceof URL)
            return (URL)basePath;
        return null;
    }

    /**
     * Convert both the descriptor and all linked Resources to JSON and return them.
     * @return
     */
    public String getJson(){
        Iterator<Resource> resourceIter = resources.iterator();
        
        JSONArray resourcesJsonArray = new JSONArray();
        while(resourceIter.hasNext()){
            Resource resource = resourceIter.next();
            resourcesJsonArray.put(resource.getJson());
        }
        
        if(resourcesJsonArray.length() > 0){
            this.jsonObject.put(JSON_KEY_RESOURCES, resourcesJsonArray);
        }

        return jsonObject.toString();
    }

    private JSONObject getJsonObject(){
        Iterator<Resource> resourceIter = resources.iterator();

        JSONArray resourcesJsonArray = new JSONArray();
        while(resourceIter.hasNext()){
            Resource resource = resourceIter.next();
            resourcesJsonArray.put(resource.getJson());
        }

        if(resourcesJsonArray.length() > 0){
            this.jsonObject.put(JSON_KEY_RESOURCES, resourcesJsonArray);
        }

        return jsonObject;
    }
    
    List<Exception> getErrors(){
        return this.errors;
    }
    
    private void setJson(JSONObject jsonObjectSource) throws Exception {
        this.jsonObject = jsonObjectSource;
        
        // Create Resource list, if there are resources.
        if(jsonObjectSource.has(JSON_KEY_RESOURCES)){
            JSONArray resourcesJsonArray = jsonObjectSource.getJSONArray(JSON_KEY_RESOURCES);
            for(int i=0; i < resourcesJsonArray.length(); i++){
                JSONObject resourceJson = resourcesJsonArray.getJSONObject(i);
                Resource resource = null;
                try {
                    resource = Resource.build(resourceJson, basePath, isArchivePackage);
                } catch (DataPackageException dpe) {
                    if(this.strictValidation){
                        this.jsonObject = null;
                        this.resources.clear();

                        throw dpe;
                        
                    }else{
                        this.errors.add(dpe);
                    }
                }

                if(resource != null){
                    addResource(resource);
                }
            }
        } else {
            DataPackageException dpe = new DataPackageException("Trying to create a DataPackage from JSON, " +
                    "but no resource entries found");
            if(this.strictValidation){
                this.jsonObject = null;
                this.resources.clear();

                throw dpe;

            }else{
                this.errors.add(dpe);
            }
        }
        Schema schema = buildSchema (jsonObjectSource, basePath, isArchivePackage);
        setFromJson(jsonObjectSource, this, schema);

        this.setName(jsonObjectSource.has(Package.JSON_KEY_ID)
                ? jsonObjectSource.getString(Package.JSON_KEY_ID)
                : null);
        this.setVersion(jsonObjectSource.has(Package.JSON_KEY_VERSION)
                ? jsonObjectSource.getString(Package.JSON_KEY_VERSION)
                : null);
        this.setHomepage(jsonObjectSource.has(Package.JSON_KEY_HOMEPAGE)
                ? new URL(jsonObjectSource.getString(Package.JSON_KEY_HOMEPAGE))
                : null);
        this.setImage(jsonObjectSource.has(Package.JSON_KEY_IMAGE)
                ? jsonObjectSource.getString(Package.JSON_KEY_IMAGE)
                : null);
        this.setCreated(jsonObjectSource.has(Package.JSON_KEY_CREATED)
                ? jsonObjectSource.getString(Package.JSON_KEY_CREATED)
                : null);
        this.setContributors(jsonObjectSource.has(Package.JSON_KEY_CONTRIBUTORS)
                ? Contributor.fromJson(jsonObjectSource.getString(Package.JSON_KEY_CONTRIBUTORS))
                : null);
        if (jsonObjectSource.has(Package.JSON_KEY_KEYWORDS)) {
            JSONArray arr = jsonObject.getJSONArray(Package.JSON_KEY_KEYWORDS);
            for (int i = 0; i < arr.length(); i++) {
                this.addKeyword(arr.getString(i));
            }
        }
        List<String> wellKnownKeys = Arrays.asList(JSON_KEY_RESOURCES, JSON_KEY_ID, JSON_KEY_VERSION,
                JSON_KEY_HOMEPAGE, JSON_KEY_IMAGE, JSON_KEY_CREATED, JSON_KEY_CONTRIBUTORS,
                JSON_KEY_KEYWORDS);
        jsonObjectSource.keySet().forEach((k) -> {
            if (!wellKnownKeys.contains(k)) {
                Object obj = jsonObjectSource.get(k);
                this.otherProperties.put(k, obj);
            }
        });
    }
    /**
     * @return the profile
     */
    @Override
    public String getProfile() {
        if (null == super.getProfile())
            return Profile.PROFILE_DATA_PACKAGE_DEFAULT;
        return super.getProfile();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getVersion() {
        if (versionParts != null) {
            return versionParts[0]+"."+versionParts[1]+"."+versionParts[2];
        } else
            return version;
    }

    /**
     * DataPackage version SHOULD be SemVer, but sloppy versions are acceptable.
     *
     * Spec: https://frictionlessdata.io/specs/patterns/index.html#data-package-version
     * @param version the version of the DataPackage
     */
    private void setVersion(String version) {
        this.version = version;
        if (StringUtils.isEmpty(version))
            return;
        String[] parts = version.replaceAll("\\w", "").split("\\.");
        if (parts.length == 3) {
            try {
                for (String part : parts) {
                    Integer.parseInt(part);
                }
                // do nothing if an exception is thrown, it's just
                // a datapacke with sloppy version.
            } catch (Exception ex) { }
            // we have a SemVer version scheme
            this.versionParts = new int[3];
            int cnt = 0;
            for (String part : parts) {
                int i = Integer.parseInt(part);
                versionParts[cnt++] = i;
            }
        }
        this.version = version;
    }

    public URL getHomepage() {
        return homepage;
    }

    private void setHomepage(URL homepage) {
        if (null == homepage)
            return;
        if (!isValidUrl(homepage))
            throw new DataPackageException("Homepage URL must be fully qualified");
        this.homepage = homepage;
    }


    public String getImage() {
        return image;
    }

    private void setImage(String image) {
        this.image = image;
    }

    public ZonedDateTime getCreated() {
        return created;
    }

    private void setCreated(ZonedDateTime created) {
        this.created = created;
    }

    private void setCreated(String created) {
        if (null == created)
            return;
        ZonedDateTime dt = ZonedDateTime.parse(created);
        setCreated(dt);
    }

    public List<Contributor> getContributors() {
        if (null == contributors)
            return null;
        return UnmodifiableList.decorate(contributors);
    }

    private void setContributors(Collection<Contributor> contributors) {
        if (null == contributors)
            return;
        this.contributors = new ArrayList<>(contributors);
    }

    public void addContributor (Contributor contributor) {
        if (null == contributor)
            return;
        if (null == contributors)
            contributors = new ArrayList<>();
        this.contributors.add(contributor);
    }

    public void removeContributor (Contributor contributor) {
        if (null == contributor)
            return;
        if (null == contributors)
            return;
        if (contributors.contains(contributor)) {
            this.contributors.remove(contributor);
        }
    }

    public Set<String> getKeywords() {
        if (null == keywords)
            return null;
        return UnmodifiableSet.decorate(keywords);
    }

    public void setKeywords(Set<String> keywords) {
        if (null == keywords)
            return;
        this.keywords = new LinkedHashSet<>(keywords);
    }

    private void addKeyword(String keyword) {
        if (null == keyword)
            return;
        if (null == keywords)
            keywords = new LinkedHashSet<>();
        this.keywords.add(keyword);
    }

    public void removeKeyword (String keyword) {
        if (null == keyword)
            return;
        if (null == keywords)
            return;
        if (keywords.contains(keyword)) {
            this.keywords.remove(keyword);
        }
    }

    public Object getOtherProperty(String key) {
        return otherProperties.get(key);
    }

    public void removeOtherProperty(String key) {
        if (null == key)
            return;
        if (null == otherProperties)
            return;
        if (otherProperties.keySet().contains(key)) {
            otherProperties.remove(key);
        }
    }

    public void setOtherProperties(String key, Object value) {
        if (null == key)
            return;
        this.otherProperties.put(key, value);
    }

    private static URL getParentUrl(URL urlSource) throws URISyntaxException, MalformedURLException {
        URI uri = urlSource.toURI();
        return (urlSource.getPath().endsWith("/")
                ? uri.resolve("..")
                : uri.resolve(".")).toURL();
    }


    /**
     * Check whether an input URL is valid according to DataPackage specs.
     *
     * From the specification: "URLs MUST be fully qualified. MUST be using either
     * http or https scheme."
     *
     * https://frictionlessdata.io/specs/data-resource/#url-or-path
     * @param url URL to test
     * @return true if the String contains a URL starting with HTTP/HTTPS
     */
    public static boolean isValidUrl(URL url) {
        return isValidUrl(url.toExternalForm());
    }

    /**
     * Check whether an input string contains a valid URL.
     *
     * From the specification: "URLs MUST be fully qualified. MUST be using either
     * http or https scheme."
     *
     * https://frictionlessdata.io/specs/data-resource/#url-or-path
     * @param objString String to test
     * @return true if the String contains a URL starting with HTTP/HTTPS
     */
    public static boolean isValidUrl(String objString) {
        String[] schemes = {"http", "https"};
        UrlValidator urlValidator = new UrlValidator(schemes);

        return urlValidator.isValid(objString);
    }
}
