package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.JSONBase;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.datasourceformat.DataSourceFormat;
import io.frictionlessdata.tableschema.io.FileReference;
import io.frictionlessdata.tableschema.io.URLFileReference;
import io.frictionlessdata.tableschema.iterator.BeanIterator;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.iterator.TableIterator;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.commons.csv.CSVFormat;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Abstract base implementation of a Resource.
 * Based on specs: http://frictionlessdata.io/specs/data-resource/
 */
public abstract class AbstractResource<T,C> extends JSONBase implements Resource<T,C> {

    // Data properties.
    protected List<Table> tables;

    // Metadata properties.
    // Required properties.
    private String name;

    // Recommended properties.
    String profile = null;

    // Optional properties.
    String title = null;
    String description = null;


    String format = null;
    String mediaType = null;
    String encoding = null;
    Integer bytes = null;
    String hash = null;

    Dialect dialect;
    JSONArray sources = null;
    JSONArray licenses = null;

    // Schema
    Schema schema = null;

    boolean serializeToFile = true;
    private String serializationFormat;

    AbstractResource(String name){
        this.name = name;
        if (null == name)
            throw new DataPackageException("Invalid Resource, it does not have a name property.");
    }

    @Override
    public Iterator<Object[]> objectArrayIterator() throws Exception{
        return this.objectArrayIterator(false, false, false);
    }

    @Override
    public Iterator<Object[]> objectArrayIterator(boolean keyed, boolean extended, boolean relations) throws Exception{
        ensureDataLoaded();
        Iterator<Object[]>[] tableIteratorArray = new TableIterator[tables.size()];
        int cnt = 0;
        for (Table table : tables) {
            tableIteratorArray[cnt++] = table.iterator(keyed, extended, true, relations);
        }
        return new IteratorChain(tableIteratorArray);
    }

    private Iterator stringArrayIterator(boolean relations) throws Exception{
        ensureDataLoaded();
        Iterator[] tableIteratorArray = new TableIterator[tables.size()];
        int cnt = 0;
        for (Table table : tables) {
            tableIteratorArray[cnt++] = table.iterator(false, false, false, relations);
        }
        return new IteratorChain<>(tableIteratorArray);
    }

    @Override
    public Iterator<String[]> stringArrayIterator() throws Exception{
        ensureDataLoaded();
        Iterator<String[]>[] tableIteratorArray = new TableIterator[tables.size()];
        int cnt = 0;
        for (Table table : tables) {
            tableIteratorArray[cnt++] = table.stringArrayIterator(false);
        }
        return new IteratorChain<>(tableIteratorArray);
    }

    @Override
    public Iterator<Map<String, Object>> mappedIterator(boolean relations) throws Exception{
        ensureDataLoaded();
        Iterator<Map<String, Object>>[] tableIteratorArray = new TableIterator[tables.size()];
        int cnt = 0;
        for (Table table : tables) {
            tableIteratorArray[cnt++] = table.keyedIterator(false, true, relations);
        }
        return new IteratorChain(tableIteratorArray);
    }

    @Override
    public Iterator<C> beanIterator(Class<C> beanType, boolean relations) throws Exception {
        ensureDataLoaded();
        IteratorChain<C> ic = new IteratorChain<>();
        for (Table table : tables) {
            ic.addIterator (table.iterator(beanType, false));
        }
        return ic;
    }

    public List<String[]> getData() throws Exception{
        List<String[]> retVal = new ArrayList<>();
        ensureDataLoaded();
        Iterator<String[]> iter = stringArrayIterator();
        while (iter.hasNext()) {
            retVal.add(iter.next());
        }
        return retVal;
    }

    /**
     * Most customizable method to retrieve all data in a Resource. Parameters match those in
     * {@link io.frictionlessdata.tableschema.Table#iterator(boolean, boolean, boolean, boolean)}. Data can be
     * returned as:
     *
     * - String arrays,
     * - as Object arrays (parameter `cast` = true),
     * - as a Map&lt;key,val&gt; where key is the header name, and val is the data (parameter `keyed` = true),
     * - or in an "extended" form (parameter `extended` = true) that returns an Object array where the first entry is the
     *      row number, the second is a String array holding the headers, and the third is an Object array holding
     *      the row data.
     * @param keyed returns data as Maps
     * @param extended returns data in "extended form"
     * @param cast returns data as Objects, not Strings
     * @param relations resolves relations
     * @return List of data objects
     * @throws Exception if reading data fails
     */
    public List<Object[]> getData(boolean keyed, boolean extended, boolean cast, boolean relations) throws Exception{
        List<Object[]> retVal = new ArrayList<>();
        ensureDataLoaded();
        Iterator iter;
        if (cast) {
            iter = objectArrayIterator(keyed, extended, relations);
        } else {
            iter = stringArrayIterator(relations);
        }
        while (iter.hasNext()) {
            retVal.add((Object[])iter.next());
        }
        return retVal;
    }


    @Override
    public List<C> getData(Class<C> beanClass)  throws Exception {
        List<C> retVal = new ArrayList<C>();
        ensureDataLoaded();
        for (Table t : tables) {
            final BeanIterator<C> iter = t.iterator(beanClass, false);
            while (iter.hasNext()) {
                retVal.add(iter.next());
            }
        }
        return retVal;
    }

    @Override
    public String[] getHeaders() throws Exception{
        ensureDataLoaded();
        return tables.get(0).getHeaders();
    }

    @Override
    public List<Table> getTables() throws Exception {
        ensureDataLoaded();
        return tables;
    }

    /**
     * Get JSON representation of the object.
     * @return a JSONObject representing the properties of this object
     */
    public String getJson(){
        //FIXME: Maybe use something lke GSON so we don't have to explicitly
        //code this...
        JSONObject json = new JSONObject(new LinkedHashMap<String, Object>());

        // Null values will not actually be "put," as per JSONObject specs.
        json.put(JSON_KEY_NAME, this.getName());

        if (this instanceof URLbasedResource) {
            json.put(JSON_KEY_PATH, ((URLbasedResource) this).getPathJson());
        } else if (this instanceof FilebasedResource) {
            if (this.shouldSerializeToFile()) {
                json.put(JSON_KEY_PATH, ((FilebasedResource) this).getPathJson());
            } else {
                try {
                    JSONArray data = new JSONArray();
                    List<Table> tables = readData();
                    for (Table t : tables) {
                        JSONArray arr = new JSONArray(t.asJson());
                        arr.toList().forEach(data::put);
                    }
                    json.put(JSON_KEY_DATA, data);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        } else if ((this instanceof AbstractDataResource)) {
            if (this.shouldSerializeToFile()) {
                //TODO implement storing only the path - and where to get it
            } else {
                json.put(JSON_KEY_DATA, ((AbstractDataResource) this).getDataPoperty());
            }
        }
        json.put(JSON_KEY_PROFILE, this.profile);
        json.put(JSON_KEY_TITLE, this.title);
        json.put(JSON_KEY_DESCRIPTION, this.description);
        json.put(JSON_KEY_FORMAT, this.format);
        json.put(JSON_KEY_MEDIA_TYPE, this.getMediaType());
        json.put(JSON_KEY_ENCODING, this.getEncoding());
        json.put(JSON_KEY_BYTES, this.getBytes());
        json.put(JSON_KEY_HASH, this.getHash());
        json.put(JSON_KEY_SOURCES, this.getSources());
        json.put(JSON_KEY_LICENSES, this.getLicenses());

        Object schemaObj = originalReferences.get(JSONBase.JSON_KEY_SCHEMA);
        if ((null == schemaObj) && (null != schema)) {
            if (null != schema.getReference()) {
                schemaObj = JSON_KEY_SCHEMA + "/" + schema.getReference().getFileName();
            }
        }
        json.put(JSON_KEY_SCHEMA, schemaObj);

        Object dialectObj = originalReferences.get(JSONBase.JSON_KEY_DIALECT);
        if ((null == dialectObj) && (null != dialect)) {
            if (null != dialect.getReference()) {
                dialectObj = JSON_KEY_DIALECT + "/" + dialect.getReference().getFileName();
            }
        }
        json.put(JSON_KEY_DIALECT, dialectObj);
        return json.toString();
    }



    public void writeSchema(Path parentFilePath) throws IOException{
        String relPath = getPathForWritingSchema();
        if (null == originalReferences.get(JSONBase.JSON_KEY_SCHEMA)) {
            originalReferences.put(JSONBase.JSON_KEY_SCHEMA, relPath);
        }

        if (null != relPath) {
            Path p;
            if (parentFilePath.toString().isEmpty()) {
                p = parentFilePath.getFileSystem().getPath(relPath);
            } else {
                p = parentFilePath.resolve(relPath);
            }
            writeSchema(p, schema);
        }
    }

    private static void writeSchema(Path parentFilePath, Schema schema) throws IOException {
        if (!Files.exists(parentFilePath)) {
            Files.createDirectories(parentFilePath);
        }
        Files.deleteIfExists(parentFilePath);
        try (Writer wr = Files.newBufferedWriter(parentFilePath, StandardCharsets.UTF_8)) {
            wr.write(schema.getJson());
        }
    }


    private static void writeDialect(Path parentFilePath, Dialect dialect) throws IOException {
        if (!Files.exists(parentFilePath)) {
            Files.createDirectories(parentFilePath);
        }
        Files.deleteIfExists(parentFilePath);
        try (Writer wr = Files.newBufferedWriter(parentFilePath, StandardCharsets.UTF_8)) {
            wr.write(dialect.getJson());
        }
    }

    /**
     * Construct a path to write out the Schema for this Resource
     * @return a String containing a relative path for writing or null
     */
    @Override
    public String getPathForWritingSchema() {
        Schema resSchema = getSchema();
        // write out schema file only if not null or URL
        FileReference ref = (null != resSchema) ? resSchema.getReference() : null;
        return getPathForWritingSchemaOrDialect(JSON_KEY_SCHEMA, resSchema, ref);
    }

    /**
     * Construct a path to write out the Dialect for this Resource
     * @return a String containing a relative path for writing or null
     */
    @Override
    public String getPathForWritingDialect() {
        Dialect dialect = getDialect();
        // write out dialect file only if not null or URL
        FileReference ref = (null != dialect) ? dialect.getReference() : null;
        return getPathForWritingSchemaOrDialect(JSON_KEY_DIALECT, dialect, ref);
    }

    /**
     * If we don't have a object, return null (nothing to serialize)
     * If we have a object, but it was read from an URL, return null (DataPackage will just use the URL)
     * If there is a object in the first place and it is not URL based or freshly created,
     * construct a relative file path for writing
     * If we have a object, but it is freshly created, and the Resource data should be written to file,
     * create a file name from the Resource name
     * If we have a object, but it is freshly created, and the Resource data should not be written to file,
     * return null
     * @return a String containing a relative path for writing or null
     */
    private String getPathForWritingSchemaOrDialect(String key, Object objectWithRes, FileReference reference) {
        // write out schema file only if not null or URL
        if (null == objectWithRes) {
            return null;
        } else if ((reference instanceof URLFileReference)){
            return null;
        } else if (getOriginalReferences().containsKey(key)) {
            return getOriginalReferences().get(key).toString();
        } else if (null != reference) {
            return key + "/" + reference.getFileName();
        } else if (this.shouldSerializeToFile()) {
            return key + "/" + name.toLowerCase().replaceAll("\\W", "")+".json";
        } else
            return null;
    }

    /**
     * @return the name
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    @Override
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the profile
     */
    @Override
    public String getProfile() {
        if (null == profile)
            return Profile.PROFILE_DATA_RESOURCE_DEFAULT;
        return profile;
    }

    /**
     * @param profile the profile to set
     */
    @Override
    public void setProfile(String profile) {
        this.profile = profile;
    }

    /**
     * @return the title
     */
    @Override
    public String getTitle() {
        return title;
    }

    /**
     * @param title the title to set
     */
    @Override
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * @return the description
     */
    @Override
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    @Override
    public void setDescription(String description) {
        this.description = description;
    }


    /**
     * @return the mediaType
     */
    @Override
    public String getMediaType() {
        return mediaType;
    }

    /**
     * @param mediaType the mediaType to set
     */
    @Override
    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }

    /**
     * @return the encoding
     */
    @Override
    public String getEncoding() {
        return encoding;
    }

    /**
     * @param encoding the encoding to set
     */
    @Override
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /**
     * @return the bytes
     */
    @Override
    public Integer getBytes() {
        return bytes;
    }

    /**
     * @param bytes the bytes to set
     */
    @Override
    public void setBytes(Integer bytes) {
        this.bytes = bytes;
    }

    /**
     * @return the hash
     */
    @Override
    public String getHash() {
        return hash;
    }

    /**
     * @param hash the hash to set
     */
    @Override
    public void setHash(String hash) {
        this.hash = hash;
    }

    /**
     * @return the dialect
     */
    @Override
    public Dialect getDialect() {
        return dialect;
    }

    /**
     * @param dialect the dialect to set
     */
    @Override
    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public String getFormat() {
        return format;
    }

    @Override
    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public Schema getSchema(){
        return this.schema;
    }

    @Override
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public String getDialectReference() {
        if (null == originalReferences.get(JSONBase.JSON_KEY_DIALECT))
            return null;
        return originalReferences.get(JSONBase.JSON_KEY_DIALECT).toString();
    }

    CSVFormat getCsvFormat() {
        Dialect lDialect = (null != dialect) ? dialect : Dialect.DEFAULT;
        return lDialect.toCsvFormat();
    }

    /**
     * @return the sources
     */
    @Override
    public JSONArray getSources() {
        return sources;
    }

    /**
     * @param sources the sources to set
     */
    @Override
    public void setSources(JSONArray sources) {
        this.sources = sources;
    }

    /**
     * @return the licenses
     */
    @Override
    public JSONArray getLicenses() {
        return licenses;
    }

    /**
     * @param licenses the licenses to set
     */
    @Override
    public void setLicenses(JSONArray licenses) {
        this.licenses = licenses;
    }


    @Override
    public boolean shouldSerializeToFile() {
        return serializeToFile;
    }

    @Override
    public void setShouldSerializeToFile(boolean serializeToFile) {
        this.serializeToFile = serializeToFile;
    }

    @Override
    public void setSerializationFormat(String format) {
        if ((format.equals(DataSourceFormat.Format.FORMAT_JSON.getLabel()))
            || format.equals(DataSourceFormat.Format.FORMAT_CSV.getLabel())) {
            this.serializationFormat = format;
        } else
            throw new DataPackageException("Serialization format "+format+" is unknown");
    }

    /**
     * if an expli
     * @return
     */
    public String getSerializationFormat() {
        if (null != serializationFormat)
            return serializationFormat;
        return format;
    }

    abstract List<Table> readData() throws Exception;

    public abstract Set<String> getDatafileNamesForWriting();

    private List<Table> ensureDataLoaded () throws Exception {
        if (null == tables) {
            tables = readData();
        }
        return tables;
    }


    @Override
    public void writeData(Path outputDir) throws Exception {
        Dialect lDialect = (null != dialect) ? dialect : Dialect.DEFAULT;
        List<Table> tables = getTables();
        Set<String> paths = getDatafileNamesForWriting();

        int cnt = 0;
        for (String fName : paths) {
            String fileName = fName+"."+getSerializationFormat();
            Table t  = tables.get(cnt++);
            Path p;
            if (outputDir.toString().isEmpty()) {
                p = outputDir.getFileSystem().getPath(fileName);
            } else {
                p = outputDir.resolve(fileName);
            }
            if (!Files.exists(p)) {
                Files.createDirectories(p);
            }
            Files.deleteIfExists(p);
            try (Writer wr = Files.newBufferedWriter(p, StandardCharsets.UTF_8)) {
                if (serializationFormat.equals(DataSourceFormat.Format.FORMAT_CSV.getLabel())) {
                    t.writeCsv(wr, lDialect.toCsvFormat());
                } else if (serializationFormat.equals(DataSourceFormat.Format.FORMAT_JSON.getLabel())) {
                    wr.write(t.asJson());
                }
            }
        }
    }

    /**
     * Write the Table as CSV into a file inside `outputDir`.
     *
     * @param outputFile the file to write to.
     * @param dialect the CSV dialect to use for writing
     * @throws Exception if something fails while writing
     */
    void writeTableAsCsv(Table t, Dialect dialect, Path outputFile) throws Exception {
        if (!Files.exists(outputFile)) {
            Files.createDirectories(outputFile);
        }
        Files.deleteIfExists(outputFile);
        try (Writer wr = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            t.writeCsv(wr, dialect.toCsvFormat());
        }
    }
}