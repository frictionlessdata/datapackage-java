package io.frictionlessdata.datapackage;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DialectTest {
    private String delimiter = ",";
    private String lineTerminator = "\r\n";
    private Character quoteChar ='"';
    private Boolean doubleQuote = true;
    private Character escapeChar = null;
    private String nullSequence = "";
    private Boolean skipInitialSpace = true;
    private Boolean hasHeaderRow = true;
    private Character commentChar = null;
    private Boolean caseSensitiveHeader = false;
    private Double csvddfVersion = 1.2;

    @Test
    @DisplayName("validate DEFAULT Dialect")
    void testDefaultDialect(){
        Dialect dia = Dialect.DEFAULT;
        Assertions.assertEquals(delimiter, dia.getDelimiter());
        Assertions.assertEquals(lineTerminator, dia.getLineTerminator());
        Assertions.assertEquals(quoteChar, dia.getQuoteChar());
        Assertions.assertEquals(doubleQuote, dia.isDoubleQuote());
        Assertions.assertEquals(escapeChar, dia.getEscapeChar());
        Assertions.assertEquals(nullSequence, dia.getNullSequence());
        Assertions.assertEquals(skipInitialSpace, dia.isSkipInitialSpace());
        Assertions.assertEquals(hasHeaderRow, dia.isHasHeaderRow());
        Assertions.assertEquals(commentChar, dia.getCommentChar());
        Assertions.assertEquals(caseSensitiveHeader, dia.isCaseSensitiveHeader());
        Assertions.assertEquals(csvddfVersion, dia.getCsvddfVersion());
    }

    @Test
    @DisplayName("test for https://github.com/frictionlessdata/datapackage-java/issues/33, NPE creating Dialect")
    void testIssue33(){
        Dialect dia = Dialect.fromCsvFormat(CSVFormat.DEFAULT);
    }


    @Test
    @DisplayName("validate custom Dialect")
    void testDialectFromJson() {
        String json = "{ "+
             " \"delimiter\":\"\t\", "+
             " \"header\":\"false\", "+
             " \"quoteChar\":\"\\\"\", " +
             " \"commentChar\":\"#\" "+
        "}";
        Dialect dia = Dialect.fromJson(json);
        Assertions.assertEquals("\t", dia.getDelimiter());
        Assertions.assertEquals('"', dia.getQuoteChar());
        Assertions.assertEquals('#', dia.getCommentChar());
        Assertions.assertFalse(dia.isHasHeaderRow());
    }

    @Test
    @DisplayName("clone Dialect")
    void testCloneDialect() throws CloneNotSupportedException {
        String json = "{ "+
                " \"delimiter\":\"\t\", "+
                " \"header\":\"false\", "+
                " \"quoteChar\":\"\\\"\", " +
                " \"commentChar\":\"#\" "+
                "}";
        Dialect dia = Dialect.fromJson(json);
        Dialect clone = dia.clone();
        Assertions.assertEquals(dia, clone);
    }

    @Test
    @DisplayName("Hashcode for Dialect")
    void testDialectHashCode() {
        String json = "{ "+
                " \"delimiter\":\"\t\", "+
                " \"header\":\"false\", "+
                " \"quoteChar\":\"\\\"\", " +
                " \"commentChar\":\"#\" "+
                "}";
        Dialect dia = Dialect.fromJson(json);

        Assertions.assertEquals(2019600303, dia.hashCode());
        Assertions.assertEquals(-1266124318, Dialect.DEFAULT.hashCode());
    }

    @Test
    @DisplayName("Hashcode for Dialect")
    void testDefaultDialectJson() {
        String defaultJson = "{\"caseSensitiveHeader\":false,\"quoteChar\":\"\\\"\",\"doubleQuote\":true," +
                "\"delimiter\":\",\",\"lineTerminator\":\"\\r\\n\",\"nullSequence\":\"\"," +
                "\"header\":true,\"csvddfVersion\":1.2,\"skipInitialSpace\":true}";
        Assertions.assertEquals(defaultJson, Dialect.DEFAULT.asJson());
    }
}
