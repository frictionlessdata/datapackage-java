package io.frictionlessdata.datapackage.datareference;

import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.datasourceformats.DataSourceFormat;
import org.apache.commons.validator.routines.UrlValidator;
import org.json.JSONArray;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface DataReference<T> {

    Collection<T> resolve();

    JSONArray  getJson();

    Collection<String> getReferencesAsStrings();


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
    static boolean isValidUrl(String objString) {
        String[] schemes = {"http", "https"};
        UrlValidator urlValidator = new UrlValidator(schemes);

        return urlValidator.isValid(objString);
    }

    static String getFileContentAsString(InputStream stream) {
        try (BufferedReader rdr = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            List<String> lines = rdr
                .lines()
                .collect(Collectors.toList());
            return String.join("\n", lines);
        }  catch (Exception ex) {
            throw new DataPackageException(ex);
        }
    }

    static String getFileContentAsString(URL url) {
        try {
            return getFileContentAsString(url.openStream());
        } catch (Exception ex) {
            throw new DataPackageException(ex);
        }
    }

    static String getFileContentAsString(File file) {
        try (InputStream is = new FileInputStream(file)){
            return getFileContentAsString(is);
        } catch (Exception ex) {
            throw new DataPackageException(ex);
        }
    }


    static DataReference build (Set<String> source, Object basePath) {
        Set<URL> urls = new LinkedHashSet<>();
        Set<File> files = new LinkedHashSet<>();
        for (String s : source) {
            try {
                if ((basePath instanceof File)) {
                    if (DataReference.isValidUrl(s)) {
                        urls.add(new URL(s));
                        continue;
                    }
                    File f = new File (s);
                    if ((((File)basePath).getName().endsWith(".zip")) && (((File)basePath).isFile())) {
                        files.add(f);
                    } else {
                        /* from the spec: "SECURITY: / (absolute path) and ../ (relative parent path)
                           are forbidden to avoid security vulnerabilities when implementing data
                           package software."

                           https://frictionlessdata.io/specs/data-resource/index.html#url-or-path
                         */
                        Path securePath = DataSourceFormat.toSecure(f.toPath(), ((File)basePath).toPath());
                        Path relativePath = ((File)basePath).toPath().relativize(securePath);
                        files.add(relativePath.toFile());
                    }
                } else {
                    if (DataReference.isValidUrl(s)) {
                        urls.add(new URL(s));
                    }  else {
                        /*TODO watch what happens with https://github.com/frictionlessdata/specs/issues/652
                         * - if URLs have to be fully qualified, replace this with code raising an error
                         * - if URL fragments are allowed, ensure the clause disallowing mixed URL/File
                         * paths still exists.
                        */
                        urls.add(new URL(((URL)basePath), s));
                    }
                }
            } catch (IOException | IllegalArgumentException e){
                throw new DataPackageException(e);
            }
        }
        /*
            From the spec: "It is NOT permitted to mix fully qualified URLs and relative paths
            in a path array: strings MUST either all be relative paths or all URLs."

            https://frictionlessdata.io/specs/data-resource/index.html#data-in-multiple-files
         */
        if ((urls.size() != 0) && (files.size() != 0)) {
            throw new DataPackageException("Resources with mixed URL/File paths are not allowed");
        }
        if (urls.size() != 0) {
            return new UrlDataReference(urls);
        } else if (files.size() != 0) {
            return new FileDataReference(files, (File)basePath);
        } else return null;
    }
}
