package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.JSONBase;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.Schema;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.iterator.TableIterator;
import org.apache.commons.collections.iterators.IteratorChain;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Abstract base implementation of a Resource.
 * Based on specs: http://frictionlessdata.io/specs/data-resource/
 */
public abstract class AbstractResource<T> extends JSONBase implements Resource<T> {

    // Data properties.
    private List<Table> tables;

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

    AbstractResource(String name){
        this.name = name;
        /*if (null == name)
            throw new DataPackageException("Invalid Resource, it does not have a name property.");*/
    }

    @Override
    public Iterator<Object[]> iter() throws Exception{
        return this.iter(false, false, true, false);
    }

    @Override
    public Iterator<Object[]> iter(boolean keyed, boolean extended, boolean cast, boolean relations) throws Exception{
        ensureDataLoaded();
        Iterator[] tableIteratorArray = new TableIterator[tables.size()];
        int cnt = 0;
        for (Table table : tables) {
            tableIteratorArray[cnt++] = table.iterator(keyed, extended, cast, relations);
        }
        return new IteratorChain(tableIteratorArray);
    }

    public List<Object[]> read() throws Exception{
        return this.read(false);
    }

    public List<Object[]> read (boolean cast) throws Exception{
        List<Object[]> retVal = new ArrayList<>();
        ensureDataLoaded();
        Iterator<Object[]> iter = iter(false, false, cast, false);
        while (iter.hasNext()) {
            retVal.add(iter.next());
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

    void setCsvFormat(Table table) {
        Dialect dia = (dialect != null)
                ? dialect
                : Dialect.DEFAULT;
        table.setCsvFormat(dia.toCsvFormat());
    }

    /**
     * Get JSON representation of the object.
     * @return a JSONObject representing the properties of this object
     */
    public JSONObject getJson(){
        //FIXME: Maybe use something lke GSON so we don't have to explicitly
        //code this...
        JSONObject json = new JSONObject(new LinkedHashMap<String, Object>());

        // Null values will not actually be "put," as per JSONObject specs.
        json.put(JSON_KEY_NAME, this.getName());

        if (this instanceof AbstractReferencebasedResource) {
            json.put(JSON_KEY_PATH, ((AbstractReferencebasedResource)this).getPathJson());
        }
        if (this instanceof AbstractDataResource) {
            json.put(JSON_KEY_DATA, ((AbstractDataResource)this).getData());
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
        json.put(JSON_KEY_SCHEMA, schemaObj);

        Object dialectObj = originalReferences.get(JSONBase.JSON_KEY_DIALECT);
        json.put(JSON_KEY_DIALECT, dialectObj);
        return json;
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

    @Override
    public String getSchemaReference(){
        return super.getSchemaReference();
    }

    public String getDialectReference() {
        if (null == originalReferences.get(JSONBase.JSON_KEY_DIALECT))
            return null;
        return originalReferences.get(JSONBase.JSON_KEY_DIALECT).toString();
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

    abstract List<Table> readData() throws Exception;

    private List<Table> ensureDataLoaded () throws Exception {
        tables = readData();
        return tables;
    }

    @Override
    public void writeTableAsCsv(Table t, Dialect dialect, Path outputFile) throws Exception {
        if (!Files.exists(outputFile)) {
            Files.createDirectories(outputFile);
        }
        Files.deleteIfExists(outputFile);
        try (Writer wr = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            t.writeCsv(wr, null);
        }
    }
}