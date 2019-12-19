package io.frictionlessdata.datapackage;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DialectTest {

    @Test
    @DisplayName("validate DEFAULT Dialect")
    void testDefaultDialect(){
        Dialect dia = Dialect.DEFAULT;
        Assertions.assertEquals(",", dia.getDelimiter());
    }

    @Test
    @DisplayName("test for https://github.com/frictionlessdata/datapackage-java/issues/33, NPE creating Dialect")
    void testIssue33(){
        Dialect dia = Dialect.fromCsvFormat(CSVFormat.DEFAULT);
    }
}
