package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.JSONBase;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.tableschema.iterator.BeanIterator;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.util.JsonUtil;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.iterator.TableIterator;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.csv.CSVFormat;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

/**
 * Abstract base implementation of a Resource.
 * Based on specs: http://frictionlessdata.io/specs/data-resource/
 */
@JsonInclude(value = Include.NON_ABSENT, content = Include.NON_ABSENT)
public abstract class AbstractResource<T,C> extends JSONBase implements Resource<T,C> {

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
    ArrayNode sources = null;
    ArrayNode licenses = null;

    // Schema
    Schema schema = null;

    AbstractResource(String name){
        this.name = name;
        /*if (null == name)
            throw new DataPackageException("Invalid Resource, it does not have a name property.");*/
    }

    @Override
    public Iterator<Object[]> objectArrayIterator() throws Exception{
        return this.objectArrayIterator(false, false, true, false);
    }

    @Override
    public Iterator<Object[]> objectArrayIterator(boolean keyed, boolean extended, boolean cast, boolean relations) throws Exception{
        ensureDataLoaded();
        Iterator<Object[]>[] tableIteratorArray = new TableIterator[tables.size()];
        int cnt = 0;
        for (Table table : tables) {
            tableIteratorArray[cnt++] = table.iterator(keyed, extended, cast, relations);
        }
        return new IteratorChain(tableIteratorArray);
    }

    @Override
    public Iterator<String[]> stringArrayIterator() throws Exception{
        ensureDataLoaded();
        Iterator<String[]>[] tableIteratorArray = new TableIterator[tables.size()];
        int cnt = 0;
        for (Table table : tables) {
            tableIteratorArray[cnt++] = table.stringArrayIterator(false);
        }
        return new IteratorChain(tableIteratorArray);
    }


    public List<Object[]> read() throws Exception{
        return this.read(false);
    }

    public List<Object[]> read (boolean cast) throws Exception{
        List<Object[]> retVal = new ArrayList<>();
        ensureDataLoaded();
        Iterator<Object[]> iter = objectArrayIterator(false, false, cast, false);
        while (iter.hasNext()) {
            retVal.add(iter.next());
        }
        return retVal;
    }

    @Override
    public List<C> read (Class<C> beanClass)  throws Exception {
        List<C> retVal = new ArrayList<C>();
        ensureDataLoaded();
        for (Table t : tables) {
            final BeanIterator<C> iter = new BeanIterator<C>(t, beanClass, true);
            while (iter.hasNext()) {
                retVal.add(iter.next());
            }
        }
        return retVal;
    }

    @Override
	@JsonIgnore
    public String[] getHeaders() throws Exception{
        ensureDataLoaded();
        return tables.get(0).getHeaders();
    }

    @Override
	@JsonIgnore
    public List<Table> getTables() throws Exception {
        ensureDataLoaded();
        return tables;
    }

    /**
     * Get JSON representation of the object.
     * @return a JSONObject representing the properties of this object
     */
    @JsonIgnore
    public ObjectNode getJson(){
        ObjectNode json = (ObjectNode) JsonUtil.getInstance().createNode(this);

        if (this instanceof AbstractReferencebasedResource) {
            json.set(JSON_KEY_PATH, ((AbstractReferencebasedResource)this).getPathJson());
        }
        if (this instanceof AbstractDataResource) {
            json.set(JSON_KEY_DATA, JsonUtil.getInstance().createNode(((AbstractDataResource)this).getData()));
        }
        if (Objects.nonNull(originalReferences.get(JSONBase.JSON_KEY_SCHEMA)) && 
        		originalReferences.get(JSONBase.JSON_KEY_SCHEMA).getClass().isAssignableFrom(ObjectNode.class)) {
        	Object schemaObj = originalReferences.get(JSONBase.JSON_KEY_SCHEMA);
            json.set(JSON_KEY_SCHEMA, JsonUtil.getInstance().createNode(schemaObj));
        }
        if (Objects.nonNull(originalReferences.get(JSONBase.JSON_KEY_DIALECT)) && 
        		originalReferences.get(JSONBase.JSON_KEY_DIALECT).getClass().isAssignableFrom(ObjectNode.class)) {
        	Object dialectObj = originalReferences.get(JSONBase.JSON_KEY_DIALECT);
            json.set(JSON_KEY_DIALECT, JsonUtil.getInstance().createNode(dialectObj));
        }
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
    @JsonIgnore
    public String getSchemaReference(){
        return super.getSchemaReference();
    }

    @JsonIgnore
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
    public ArrayNode getSources() {
        return sources;
    }

    /**
     * @param sources the sources to set
     */
    @Override
    public void setSources(ArrayNode sources) {
        this.sources = sources;
    }

    /**
     * @return the licenses
     */
    @Override
    public ArrayNode getLicenses() {
        return licenses;
    }

    /**
     * @param licenses the licenses to set
     */
    @Override
    public void setLicenses(ArrayNode licenses) {
        this.licenses = licenses;
    }

    abstract List<Table> readData() throws Exception;

    private List<Table> ensureDataLoaded () throws Exception {
        if (null == tables) {
            tables = readData();
        }
        return tables;
    }

    @Override
    public void writeTableAsCsv(Table t, Dialect dialect, Path outputFile) throws Exception {
        if (!Files.exists(outputFile)) {
            Files.createDirectories(outputFile);
        }
        Files.deleteIfExists(outputFile);
        try (Writer wr = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            t.writeCsv(wr, dialect.toCsvFormat());
        }
    }
}