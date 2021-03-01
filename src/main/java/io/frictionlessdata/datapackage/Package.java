package io.frictionlessdata.datapackage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.resource.AbstractDataResource;
import io.frictionlessdata.datapackage.resource.AbstractReferencebasedResource;
import io.frictionlessdata.datapackage.resource.Resource;
import io.frictionlessdata.tableschema.exception.ValidationException;
import io.frictionlessdata.tableschema.io.LocalFileReference;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.collections.set.UnmodifiableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;

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
import java.util.zip.ZipOutputStream;


/**
 * Load, validate, create, and save a datapackage object according to the specifications at
 * https://github.com/frictionlessdata/specs/blob/master/specs/data-package.md
 */
@JsonInclude(value = Include.NON_EMPTY, content = Include.NON_EMPTY )
public class Package extends JSONBase{
    public  static final String DATAPACKAGE_FILENAME = "datapackage.json";
    private static final String JSON_KEY_RESOURCES = "resources";
    private static final String JSON_KEY_ID = "id";
    private static final String JSON_KEY_VERSION = "version";
    private static final String JSON_KEY_HOMEPAGE = "homepage";
    private static final String JSON_KEY_KEYWORDS = "keywords";
    private static final String JSON_KEY_IMAGE = "image";
    private static final String JSON_KEY_CREATED = "created";
    private static final String JSON_KEY_CONTRIBUTORS = "contributors";

    // Filesystem path pointing to the Package; either ZIP file or directory
    private Object basePath = null;
    private String id;
    private String version;
    private int[] versionParts;
    private URL homepage;
    private Set<String> keywords = new TreeSet<>();
    private String image;
    private ZonedDateTime created;
    private List<Contributor> contributors = new ArrayList<>();
    
    private ObjectNode jsonObject = JsonUtil.getInstance().createNode();
    private boolean strictValidation = false;
    private final List<Resource> resources = new ArrayList<>();
    private final List<Exception> errors = new ArrayList<>();
    private final Validator validator = new Validator();

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
            addResource(r, false);
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

    	// Create and set the JsonNode for the String representation of descriptor JSON object.
        this.setJson((ObjectNode) JsonUtil.getInstance().createNode(jsonStringSource));
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

        // Create JsonNode and validate.
        try {
        	this.setJson((ObjectNode) createNode(jsonString));
        } catch (DataPackageException ex) {
        	if (strict) {
        		throw ex;
        	}
        }
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
        JsonNode sourceJsonNode;
        if (descriptorFile.toFile().getName().toLowerCase().endsWith(".zip")) {
            isArchivePackage = true;
            basePath = descriptorFile;
        	sourceJsonNode = createNode(JSONBase.getZipFileContentAsString(descriptorFile, DATAPACKAGE_FILENAME));
        } else {
            basePath = descriptorFile.getParent();
            String sourceJsonString = getFileContentAsString(descriptorFile);
            sourceJsonNode = createNode(sourceJsonString);
        }
        this.setJson((ObjectNode) sourceJsonNode);
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
            writer.write(this.getJsonNode().toPrettyString());
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
            r.writeData(outFs.getPath(parentDirName+File.separator));
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
        FileSystem outFs = getTargetFileSystem(outputDir, zipCompressed);
        String parentDirName = "";
        if (!zipCompressed) {
            parentDirName = outputDir.getPath();
        }

        // only file-based Resources need to be written to the DataPackage, URLs stay as
        // external references and JSONArray-based Resources got serialized as part of the
        // Descriptor file
        final List<Resource> resourceList = resources
                .stream()
                .filter(Resource::shouldSerializeToFile)
                .collect(Collectors.toList());

        for (Resource r : resourceList) {
            r.writeData(outFs.getPath(parentDirName ));
            r.writeSchema(outFs.getPath(parentDirName));

            // write out dialect file only if not null or URL
            String dialectP = r.getPathForWritingDialect();
            if (null != dialectP) {
                Path dialectPath = outFs.getPath(parentDirName + File.separator + dialectP);
                writeDialect(dialectPath, r.getDialect());
            }
            Dialect dia = r.getDialect();
            if (null != dia) {
                dia.setReference(new LocalFileReference(new File(dialectP)));
            }
        }
        writeDescriptor(outFs, parentDirName);
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
            file.write(this.getJsonNode().toPrettyString());
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
                    zos.write(this.getJsonNode().toPrettyString().getBytes());
                    zos.closeEntry();
                }
            }
        }
    }

    /*
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

        */
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
        addResource(resource, true);
    }

    private void addResource(Resource resource, boolean validate)
            throws IOException, ValidationException, DataPackageException{
        DataPackageException dpe = null;
        if (resource.getName() == null){
            dpe = new DataPackageException("Invalid Resource, it does not have a name property.");
        }
        if (resource instanceof AbstractDataResource)
            addResource((AbstractDataResource) resource, validate);
        else if (resource instanceof AbstractReferencebasedResource)
            addResource((AbstractReferencebasedResource) resource, validate);
        if (validate)
            validate(dpe);
    }

    private void addResource(AbstractDataResource resource, boolean validate)
            throws IOException, ValidationException, DataPackageException{
        DataPackageException dpe = null;
        // If a name property isn't given...
        if ((resource.getDataProperty() == null) || (resource).getFormat() == null) {
            dpe = new DataPackageException("Invalid Resource. The data and format properties cannot be null.");
        } else {
            dpe = checkDuplicates(resource);
        }
        if (validate)
            validate(dpe);

        this.resources.add(resource);
    }

    private void addResource(AbstractReferencebasedResource resource, boolean validate)
            throws IOException, ValidationException, DataPackageException{
        DataPackageException dpe = null;
        if (resource.getPaths() == null) {
            dpe = new DataPackageException("Invalid Resource. The path property cannot be null.");
        } else {
            dpe = checkDuplicates(resource);
        }
        if (validate)
            validate(dpe);

        this.resources.add(resource);
    }

    void removeResource(String name){
        this.resources.removeIf(resource -> resource.getName().equalsIgnoreCase(name));
    }

    /**
     * Add a new property and value to the Package. If a value already is defined for the key,
     * an exception is thrown. The value can be either a plain string or a string holding a JSON-Array or
     * JSON-object.
     * @param key the property name
     * @param value the value to set.
     * @throws DataPackageException if the property denoted by `key` already exists
     */
    public void addProperty(String key, String value) throws DataPackageException{
        if(this.jsonObject.has(key)){
            throw new DataPackageException("A property with the same key already exists.");
        }else{
        	this.jsonObject.put(key, value);
        }
    }

    public void setProperty(String key, JsonNode value) throws DataPackageException{
    	this.jsonObject.set(key, value);
    }

    public void addProperty(String key, JsonNode value) throws DataPackageException{
        if(this.jsonObject.has(key)){
            throw new DataPackageException("A property with the same key already exists.");
        }else{
        	this.jsonObject.set(key, value);
        }
    }
    
    public Object getProperty(String key) {
        if (!this.jsonObject.has(key)) {
            return null;
        }
        return jsonObject.get(key);
    }

    public void removeProperty(String key){
        this.getJsonNode().remove(key);
    }
    
    /**
     * Validation is strict or unstrict depending on how the package was
     * instantiated with the strict flag.
     * @throws IOException
     * @throws DataPackageException
     */
    final void validate() throws IOException, DataPackageException{
        try{
            this.validator.validate(this.getJsonNode());
        }catch(ValidationException | DataPackageException ve){
            if(this.strictValidation){
                throw ve;
            }else{
                errors.add(ve);
            }
        }
    }
    
    @JsonIgnore
    final Path getBasePath(){
        if (basePath instanceof File)
            return ((File)this.basePath).toPath();
        else if (basePath instanceof Path)
            return (Path) basePath;
        else
            return null;
    }

    @JsonIgnore
    final URL getBaseUrl(){
        if (basePath instanceof URL)
            return (URL)basePath;
        return null;
    }

    /**
     * Convert both the descriptor and all linked Resources to JSON and return them.
     * @return JSON-String representation of the Package
     */
    @JsonIgnore
    public String getJson(){
        return getJsonNode().toPrettyString();
    }

    @JsonIgnore
    protected ObjectNode getJsonNode(){
    	ObjectNode objectNode = (ObjectNode) JsonUtil.getInstance().createNode(this);
    	// update any manually set properties
    	this.jsonObject.fields().forEachRemaining(f->{
    		objectNode.set(f.getKey(), f.getValue());
    	});

    	Iterator<Resource> resourceIter = resources.iterator();

        ArrayNode resourcesJsonArray = JsonUtil.getInstance().createArrayNode();
        while(resourceIter.hasNext()){
            Resource resource = resourceIter.next();
            // this is ugly. If we encounter a DataResource which should be written to a file via
            // manual setting, do some trickery to not write the DataResource, but a curated version
            // to the package descriptor.
            ObjectNode obj = (ObjectNode) JsonUtil.getInstance().createNode(resource.getJson());
            if ((resource instanceof AbstractDataResource) && (resource.shouldSerializeToFile())) {
                Set<String> datafileNames = resource.getDatafileNamesForWriting();
                Set<String> outPaths = datafileNames.stream().map((r) -> r+"."+resource.getSerializationFormat()).collect(Collectors.toSet());
                if (outPaths.size() == 1) {
                    obj.put("path", outPaths.iterator().next());
                } else {
                    obj.set("path", JsonUtil.getInstance().createArrayNode(outPaths));
                }
                obj.put("format", resource.getSerializationFormat());
            }
            obj.remove("originalReferences");
            resourcesJsonArray.add(obj);
        }

        if(resourcesJsonArray.size() > 0){
        	objectNode.set(JSON_KEY_RESOURCES, resourcesJsonArray);
        }

        return objectNode;
    }
    
    List<Exception> getErrors(){
        return this.errors;
    }
    
    private void setJson(ObjectNode jsonNodeSource) throws Exception {
        this.jsonObject = jsonNodeSource;
        
        // Create Resource list, if there are resources.
        if(jsonNodeSource.has(JSON_KEY_RESOURCES) && jsonNodeSource.get(JSON_KEY_RESOURCES).isArray()){
            ArrayNode resourcesJsonArray = (ArrayNode) jsonNodeSource.get(JSON_KEY_RESOURCES);
            for(int i=0; i < resourcesJsonArray.size(); i++){
                ObjectNode resourceJson = (ObjectNode) resourcesJsonArray.get(i);
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
                    addResource(resource, false);
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
        Schema schema = buildSchema (jsonNodeSource, basePath, isArchivePackage);
        setFromJson(jsonNodeSource, this, schema);

        this.setName(textValueOrNull(jsonNodeSource, Package.JSON_KEY_ID));
        this.setVersion(textValueOrNull(jsonNodeSource, Package.JSON_KEY_VERSION));
        this.setHomepage(jsonNodeSource.has(Package.JSON_KEY_HOMEPAGE)
                ? new URL(jsonNodeSource.get(Package.JSON_KEY_HOMEPAGE).asText())
                : null);
        this.setImage(textValueOrNull(jsonNodeSource, Package.JSON_KEY_IMAGE));
        this.setCreated(textValueOrNull(jsonNodeSource, Package.JSON_KEY_CREATED));
        this.setContributors(jsonNodeSource.has(Package.JSON_KEY_CONTRIBUTORS)
                ? Contributor.fromJson(jsonNodeSource.get(Package.JSON_KEY_CONTRIBUTORS).asText())
                : null);
        if (jsonNodeSource.has(Package.JSON_KEY_KEYWORDS)) {
            ArrayNode arr = (ArrayNode) jsonObject.get(Package.JSON_KEY_KEYWORDS);
            for (int i = 0; i < arr.size(); i++) {
                this.addKeyword(arr.get(i).asText());
            }
        }
        List<String> wellKnownKeys = Arrays.asList(JSON_KEY_RESOURCES, JSON_KEY_ID, JSON_KEY_VERSION,
                JSON_KEY_HOMEPAGE, JSON_KEY_IMAGE, JSON_KEY_CREATED, JSON_KEY_CONTRIBUTORS,
                JSON_KEY_KEYWORDS);
        jsonNodeSource.fieldNames().forEachRemaining((k) -> {
            if (!wellKnownKeys.contains(k)) {
                JsonNode obj = jsonNodeSource.get(k);
                this.setProperty(k, obj);
            }
        });
        validate();
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

    private static String textValueOrNull(JsonNode source, String fieldName) {
    	return source.has(fieldName) ? source.get(fieldName).asText() : null;
    }
}
