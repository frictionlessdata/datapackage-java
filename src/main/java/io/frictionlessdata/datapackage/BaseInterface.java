package io.frictionlessdata.datapackage;

import java.util.List;

public interface BaseInterface {

    /**
     * @return the name
     */
    String getName();

    /**
     * @param name the name to set
     */
    void setName(String name);

    /**
     * @return the profile
     */
    String getProfile();

    /**
     * @param profile the profile to set
     */
    void setProfile(String profile);

    /**
     * @return the title
     */
    String getTitle();

    /**
     * @param title the title to set
     */
    void setTitle(String title);

    /**
     * @return the description
     */
    String getDescription();

    /**
     * @param description the description to set
     */
    void setDescription(String description);


    /**
     * @return the sources
     */
    List<Source> getSources();

    /**
     * @param sources the sources to set
     */
    void setSources(List<Source> sources);

    /**
     * @return the licenses
     */
    List<License> getLicenses();

    /**
     * @param licenses the licenses to set
     */
    void setLicenses(List<License> licenses);

    /**
     * @return the encoding
     */
    String getEncoding();

    /**
     * @param encoding the encoding to set
     */
    void setEncoding(String encoding);

    /**
     * @return the bytes
     */
    Integer getBytes();

    /**
     * @param bytes the bytes to set
     */
    void setBytes(Integer bytes);

    /**
     * @return the hash
     */
    String getHash();

    /**
     * @param hash the hash to set
     */
    void setHash(String hash);

    /**
     * @return the mediaType
     */
    String getMediaType();

    /**
     * @param mediaType the mediaType to set
     */
    void setMediaType(String mediaType);

}
