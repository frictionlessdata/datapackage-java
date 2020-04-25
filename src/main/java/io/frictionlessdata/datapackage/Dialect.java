package io.frictionlessdata.datapackage;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import com.fasterxml.jackson.databind.JsonNode;

import io.frictionlessdata.tableschema.util.JsonUtil;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * CSV Dialect defines a simple format to describe the various dialects of CSV files in a language agnostic
 * manner. It aims to deal with a reasonably large subset of the features which differ between dialects,
 * such as terminator strings, quoting rules, escape rules and so on.
 *
 * CSV Dialect has nothing to do with the names, contents or types of the headers or data within the CSV file,
 * only how it is formatted. However, CSV Dialect does allow the presence or absence
 * of a hasHeaderRow to be specified, similarly to [RFC4180](https://www.ietf.org/rfc/rfc4180.txt).
 *
 * According to specification: https://frictionlessdata.io/specs/csv-dialect/
 */

public class Dialect {
    // we construct one instance that will always keep the default values
    public static Dialect DEFAULT = new Dialect(){
        private JsonNode JsonNode;

        public String getJson() {
            lazyCreate();
            return JsonNode.toString();
        }

        Object get(String key) {
            lazyCreate();
            return JsonNode.get(key);
        }

        private void lazyCreate() {
            if (null == JsonNode) {
                Dialect newDialect = new Dialect();
                JsonNode = newDialect.getJsonNode(false);
            }
        }
    };
    /**
     *  specifies the character sequence which should separate fields (aka columns). Default = ,. Example \t.
     */
    private String delimiter = ",";

    /**
     * specifies the character sequence which should terminate rows. Default = \r\n
     */
    private String lineTerminator = "\r\n";

    /**
     * specifies a one-character string to use as the quoting character. Default = "
     */
    private Character quoteChar ='"';

    /**
     * controls the handling of quotes inside fields. If true, two consecutive quotes should be
     * interpreted as one. Default = true
     */
    private Boolean doubleQuote = true;

    /**
     * specifies a one-character string to use for escaping (for example, \), mutually exclusive
     * with quoteChar. Not set by default
     */
    private Character escapeChar = null;

    /**
     * specifies the null sequence (for example \N). Not set by default
     */
    private String nullSequence = null;

    /**
     * specifies how to interpret whitespace which immediately follows a delimiter;
     * if false, it means that whitespace immediately after a delimiter
     * should be treated as part of the following field. Default = true
     */
    private Boolean skipInitialSpace = true;

    /**
     * indicates whether the file includes a header row. If true the first row in
     * the file is a header row, not data. Default = true
     */
    private Boolean hasHeaderRow = true;

    /**
     * indicates a one-character string to indicate lines whose remainder should be ignored
     */
    private Character commentChar = null;

    /**
     * indicates that case in the header is meaningful. For example, columns CAT
     * and Cat should not be equated. Default = false
     */
    private Boolean caseSensitiveHeader = false;

    /**
     * a number, in n.n format, e.g., 1.2. If not present, consumers should assume latest schema version.
     */
    private Double csvddfVersion = 1.2;

    public Dialect clone() {
        Dialect retVal = new Dialect();
        retVal.delimiter = this.delimiter;
        retVal.escapeChar = this.escapeChar;
        retVal.skipInitialSpace = this.skipInitialSpace;
        retVal.nullSequence = this.nullSequence;
        retVal.commentChar = this.commentChar;
        retVal.hasHeaderRow = this.hasHeaderRow;
        retVal.lineTerminator = this.lineTerminator;
        retVal.quoteChar = this.quoteChar;
        retVal.doubleQuote = this.doubleQuote;
        retVal.caseSensitiveHeader = this.caseSensitiveHeader;
        retVal.csvddfVersion = this.csvddfVersion;
        return retVal;
    }

    // will fail for multi-character delimiters. Oh my...
    public CSVFormat toCsvFormat() {
        CSVFormat format = CSVFormat.DEFAULT
                .withDelimiter(delimiter.charAt(0))
                .withEscape(escapeChar)
                .withIgnoreSurroundingSpaces(skipInitialSpace)
                .withNullString(nullSequence)
                .withCommentMarker(commentChar)
                .withSkipHeaderRecord(!hasHeaderRow)
                .withQuote(quoteChar)
                .withQuoteMode(doubleQuote ? QuoteMode.MINIMAL : QuoteMode.NONE);
        if (hasHeaderRow)
            format = format.withHeader();
        return format;
    }

    public static Dialect fromCsvFormat(CSVFormat format) {
        Dialect dialect = new Dialect();
        dialect.setDelimiter(format.getDelimiter()+"");
        dialect.setEscapeChar(format.getEscapeCharacter());
        dialect.setSkipInitialSpace(format.getIgnoreSurroundingSpaces());
        dialect.setNullSequence(format.getNullString());
        dialect.setCommentChar(format.getCommentMarker());
        dialect.setHasHeaderRow(!format.getSkipHeaderRecord());
        dialect.setQuoteChar(format.getQuoteCharacter());
        if (null != format.getQuoteMode()) {
            dialect.setDoubleQuote(format.getQuoteMode().equals(QuoteMode.MINIMAL));
        }
        return dialect;
    }

    /**
     * Create a new Dialect object from a JSON representation
     * @param json JSON as String representation, eg. from Resource definition
     * @return new Dialect object with values from JsonNode
     */
    public static Dialect fromJson(String json) {
        if (null == json)
            return null;
        JsonNode jsonObj = JsonUtil.getInstance().createNode(json);
        Dialect dialect = new Dialect();
        if (jsonObj.has("delimiter")) {
            dialect.setDelimiter(jsonObj.get("delimiter").asText());
        }
        if (jsonObj.has("escapeChar"))
            dialect.setEscapeChar(jsonObj.get("escapeChar").asText().charAt(0));
        if (jsonObj.has("skipInitialSpace"))
            dialect.setSkipInitialSpace(jsonObj.get("skipInitialSpace").asBoolean());
        if (jsonObj.has("nullSequence"))
            dialect.setNullSequence(jsonObj.get("nullSequence").asText());
        if (jsonObj.has("commentChar"))
            dialect.setCommentChar(jsonObj.get("commentChar").asText().charAt(0));
        if (jsonObj.has("header"))
            dialect.setHasHeaderRow(jsonObj.get("header").asBoolean());
        if (jsonObj.has("lineTerminator"))
            dialect.setLineTerminator(jsonObj.get("lineTerminator").asText());
        if (jsonObj.has("quoteChar"))
            dialect.setQuoteChar(jsonObj.get("quoteChar").asText().charAt(0));
        if (jsonObj.has("doubleQuote"))
            dialect.setDoubleQuote(jsonObj.get("doubleQuote").asBoolean());
        if (jsonObj.has("caseSensitiveHeader"))
            dialect.setCaseSensitiveHeader(jsonObj.get("caseSensitiveHeader").asBoolean());
        if (jsonObj.has("csvddfVersion"))
            dialect.setCsvddfVersion(jsonObj.get("csvddfVersion").asDouble());
        return dialect;
    }

    /**
     * Get JSON representation of the object.
     * @return a String representing the properties of this object encoded as JSON
     */
    public String getJson() {
        return getJsonNode(true).toString();
    }

    private JsonNode getJsonNode(boolean checkDefault) {
        HashMap<String, Object> retVal = new HashMap<>();
        setProperty(retVal, "delimiter", delimiter, checkDefault);
        setProperty(retVal, "escapeChar", escapeChar, checkDefault);
        setProperty(retVal, "skipInitialSpace", skipInitialSpace, checkDefault);
        setProperty(retVal, "nullSequence", nullSequence, checkDefault);
        setProperty(retVal, "commentChar", commentChar, checkDefault);
        setProperty(retVal, "header", hasHeaderRow, checkDefault);
        setProperty(retVal, "lineTerminator", lineTerminator, checkDefault);
        setProperty(retVal, "quoteChar", quoteChar, checkDefault);
        setProperty(retVal, "doubleQuote", doubleQuote, checkDefault);
        setProperty(retVal, "caseSensitiveHeader", caseSensitiveHeader, checkDefault);
        setProperty(retVal, "csvddfVersion", csvddfVersion, checkDefault);
        return JsonUtil.getInstance().convertValue(retVal, JsonNode.class);
    }

    public void writeJson (File outputFile) throws IOException{
        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            writeJson(fos);
        }
    }

    public void writeJson (OutputStream output) throws IOException{
        try (BufferedWriter file = new BufferedWriter(new OutputStreamWriter(output))) {
            file.write(this.getJson());
        }
    }
    Object get(String key) {
        JsonNode obj = getJsonNode(true);
        return obj.get(key);
    }

    void setProperty(Map<String, Object> obj, String key, Object value, boolean checkDefault) {
        if (checkDefault) {
            if ((value != null) && (value != DEFAULT.get(key))) {
                obj.put(key, value);
            }
        } else {
            obj.put(key, value);
        }
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getLineTerminator() {
        return lineTerminator;
    }

    public Character getQuoteChar() {
        return quoteChar;
    }

    public boolean isDoubleQuote() {
        return doubleQuote;
    }

    public Character getEscapeChar() {
        return escapeChar;
    }

    public String getNullSequence() {
        return nullSequence;
    }

    public boolean isSkipInitialSpace() {
        return skipInitialSpace;
    }

    public boolean isHasHeaderRow() {
        return hasHeaderRow;
    }

    public Character getCommentChar() {
        return commentChar;
    }

    public boolean isCaseSensitiveHeader() {
        return caseSensitiveHeader;
    }

    public Double getCsvddfVersion() {
        return csvddfVersion;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void setLineTerminator(String lineTerminator) {
        this.lineTerminator = lineTerminator;
    }

    public void setQuoteChar(Character quoteChar) {
        this.quoteChar = quoteChar;
    }

    public void setDoubleQuote(boolean doubleQuote) {
        this.doubleQuote = doubleQuote;
    }

    public void setEscapeChar(Character escapeChar) {
        this.escapeChar = escapeChar;
    }

    public void setNullSequence(String nullSequence) {
        this.nullSequence = nullSequence;
    }

    public void setSkipInitialSpace(boolean skipInitialSpace) {
        this.skipInitialSpace = skipInitialSpace;
    }

    public void setHasHeaderRow(boolean hasHeaderRow) {
        this.hasHeaderRow = hasHeaderRow;
    }

    public void setCommentChar(Character commentChar) {
        this.commentChar = commentChar;
    }

    public void setCaseSensitiveHeader(boolean caseSensitiveHeader) {
        this.caseSensitiveHeader = caseSensitiveHeader;
    }

    public void setCsvddfVersion(Double csvddfVersion) {
        this.csvddfVersion = csvddfVersion;
    }

    public boolean equals(final Object o) {
        if (o == this) return true;
        if (!(o instanceof Dialect)) return false;
        final Dialect other = (Dialect) o;
        if (!other.canEqual((Object) this)) return false;
        final Object this$delimiter = this.getDelimiter();
        final Object other$delimiter = other.getDelimiter();
        if (this$delimiter == null ? other$delimiter != null : !this$delimiter.equals(other$delimiter)) return false;
        final Object this$lineTerminator = this.getLineTerminator();
        final Object other$lineTerminator = other.getLineTerminator();
        if (this$lineTerminator == null ? other$lineTerminator != null : !this$lineTerminator.equals(other$lineTerminator))
            return false;
        final Object this$quoteChar = this.getQuoteChar();
        final Object other$quoteChar = other.getQuoteChar();
        if (this$quoteChar == null ? other$quoteChar != null : !this$quoteChar.equals(other$quoteChar)) return false;
        final Object this$doubleQuote = this.doubleQuote;
        final Object other$doubleQuote = other.doubleQuote;
        if (this$doubleQuote == null ? other$doubleQuote != null : !this$doubleQuote.equals(other$doubleQuote))
            return false;
        final Object this$escapeChar = this.getEscapeChar();
        final Object other$escapeChar = other.getEscapeChar();
        if (this$escapeChar == null ? other$escapeChar != null : !this$escapeChar.equals(other$escapeChar))
            return false;
        final Object this$nullSequence = this.getNullSequence();
        final Object other$nullSequence = other.getNullSequence();
        if (this$nullSequence == null ? other$nullSequence != null : !this$nullSequence.equals(other$nullSequence))
            return false;
        final Object this$skipInitialSpace = this.skipInitialSpace;
        final Object other$skipInitialSpace = other.skipInitialSpace;
        if (this$skipInitialSpace == null ? other$skipInitialSpace != null : !this$skipInitialSpace.equals(other$skipInitialSpace))
            return false;
        final Object this$hasHeaderRow = this.hasHeaderRow;
        final Object other$hasHeaderRow = other.hasHeaderRow;
        if (this$hasHeaderRow == null ? other$hasHeaderRow != null : !this$hasHeaderRow.equals(other$hasHeaderRow))
            return false;
        final Object this$commentChar = this.getCommentChar();
        final Object other$commentChar = other.getCommentChar();
        if (this$commentChar == null ? other$commentChar != null : !this$commentChar.equals(other$commentChar))
            return false;
        final Object this$caseSensitiveHeader = this.caseSensitiveHeader;
        final Object other$caseSensitiveHeader = other.caseSensitiveHeader;
        if (this$caseSensitiveHeader == null ? other$caseSensitiveHeader != null : !this$caseSensitiveHeader.equals(other$caseSensitiveHeader))
            return false;
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof Dialect;
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $delimiter = this.getDelimiter();
        result = result * PRIME + ($delimiter == null ? 43 : $delimiter.hashCode());
        final Object $lineTerminator = this.getLineTerminator();
        result = result * PRIME + ($lineTerminator == null ? 43 : $lineTerminator.hashCode());
        final Object $quoteChar = this.getQuoteChar();
        result = result * PRIME + ($quoteChar == null ? 43 : $quoteChar.hashCode());
        final Object $doubleQuote = this.doubleQuote;
        result = result * PRIME + ($doubleQuote == null ? 43 : $doubleQuote.hashCode());
        final Object $escapeChar = this.getEscapeChar();
        result = result * PRIME + ($escapeChar == null ? 43 : $escapeChar.hashCode());
        final Object $nullSequence = this.getNullSequence();
        result = result * PRIME + ($nullSequence == null ? 43 : $nullSequence.hashCode());
        final Object $skipInitialSpace = this.skipInitialSpace;
        result = result * PRIME + ($skipInitialSpace == null ? 43 : $skipInitialSpace.hashCode());
        final Object $hasHeaderRow = this.hasHeaderRow;
        result = result * PRIME + ($hasHeaderRow == null ? 43 : $hasHeaderRow.hashCode());
        final Object $commentChar = this.getCommentChar();
        result = result * PRIME + ($commentChar == null ? 43 : $commentChar.hashCode());
        final Object $caseSensitiveHeader = this.caseSensitiveHeader;
        result = result * PRIME + ($caseSensitiveHeader == null ? 43 : $caseSensitiveHeader.hashCode());
        return result;
    }
}