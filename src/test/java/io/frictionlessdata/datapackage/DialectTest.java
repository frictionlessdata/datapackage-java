package io.frictionlessdata.datapackage;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;

public class DialectTest {

    @Test
    void testIssue33(){
        Dialect dia = Dialect.fromCsvFormat(CSVFormat.DEFAULT);
    }
}
