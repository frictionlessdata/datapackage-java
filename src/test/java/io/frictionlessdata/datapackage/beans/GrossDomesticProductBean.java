package io.frictionlessdata.datapackage.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.math.BigDecimal;
import java.time.Year;

@JsonPropertyOrder({
        "Country Name", "Country Code", "Year", "Value"
})
public class GrossDomesticProductBean {

    //CSV header columns:
    // Country Name,Country Code,Year,Value


    @JsonProperty("Country Name")
    String countryName;

    @JsonProperty("Country Code")
    String countryCode;

    @JsonProperty("Year")
    Year year;

    @JsonProperty("Value")
    BigDecimal amount;

    @Override
    public String toString() {
        return "GrossDomesticProductBean{" +
                "countryName='" + countryName + '\'' +
                ", countryCode='" + countryCode + '\'' +
                ", year=" + year +
                ", amount=" + amount +
                '}';
    }
}
