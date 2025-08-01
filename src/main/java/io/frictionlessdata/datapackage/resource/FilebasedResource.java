package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


@JsonInclude(value = Include.NON_EMPTY, content = Include.NON_EMPTY )
public class FilebasedResource extends AbstractReferencebasedResource<File> {
    @JsonIgnore
    private File basePath;

    @JsonIgnore
    private boolean isInArchive;


    /**
     * The charset (encoding) for writing
     */
    @JsonIgnore
    private final Charset charset = StandardCharsets.UTF_8;

    public FilebasedResource(String name, Collection<File> paths, File basePath, Charset encoding) {
        super(name, paths);
        this.encoding = encoding.name();
        if (null == paths) {
            throw new DataPackageValidationException("Invalid Resource. " +
                    "The path property cannot be null for file-based Resources.");
        }
        String format = sniffFormat(paths);
        if (format.equals(TableDataSource.Format.FORMAT_JSON.getLabel())
            || format.equals(TableDataSource.Format.FORMAT_CSV.getLabel())) {
            this.setSerializationFormat(format);
        } else {
            super.setFormat(format);
        }

        this.basePath = basePath;
        for (File path : paths) {
            /* from the spec: "SECURITY: / (absolute path) and ../ (relative parent path)
               are forbidden to avoid security vulnerabilities when implementing data
               package software."

               https://frictionlessdata.io/specs/data-resource/index.html#url-or-path
             */
            if (path.isAbsolute()) {
                throw new DataPackageValidationException("Path entries for file-based Resources cannot be absolute");
            }
        }
        serializeToFile = true;
    }

    public FilebasedResource(String name, Collection<File> paths, File basePath) {
        this(name, paths, basePath, Charset.defaultCharset());
    }


    @JsonIgnore
    public String getSerializationFormat() {
        if (null != serializationFormat)
            return serializationFormat;
        if (null == format) {
            return format;
        }
        return sniffFormat(paths);
    }

    public static FilebasedResource fromSource(String name, Collection<File> paths, File basePath, Charset encoding) {
        FilebasedResource r = new FilebasedResource(name, paths, basePath);
        r.encoding = encoding.name();
        return r;
    }

    @JsonIgnore
    public File getBasePath() {
        return basePath;
    }

    @Override
    @JsonIgnore
    byte[] getRawData(File input)  throws IOException {
        if (this.isInArchive) {
            String fileName = input.getPath().replaceAll("\\\\", "/");
            return getZipFileContentAsString (basePath.toPath(), fileName).getBytes(charset);
        } else {
            File file = new File(this.basePath, input.getPath());
            try (InputStream inputStream = Files.newInputStream(file.toPath())) {
                return getRawData(inputStream);
            }
        }

    }

    @Override
    Table createTable(File reference) {
        return Table.fromSource(reference, basePath, schema, getCsvFormat());
    }

    @Override
    String getStringRepresentation(File reference) {
        if (File.separator.equals("\\"))
            return reference.getPath().replaceAll("\\\\", "/");
        return reference.getPath();
    }

    @Override
    @JsonIgnore
    public String[] getHeaders() throws Exception{
        if ((null != profile) && (profile.equals(Profile.PROFILE_DATA_PACKAGE_DEFAULT))) {
            return null;
        }
        ensureDataLoaded();
        return tables.get(0).getHeaders();
    }

    @Override
    @JsonIgnore
    List<Table> readData () throws Exception{
        List<Table> tables;
        if (this.isInArchive) {
            tables = readfromZipFile();
        } else {
            tables = readfromOrdinaryFile();
        }
        return tables;
    }

    private List<Table> readfromZipFile() throws IOException {
        List<Table> tables = new ArrayList<>();
        for (File file : paths) {
            String fileName = file.getPath().replaceAll("\\\\", "/");
            String content = getZipFileContentAsString (basePath.toPath(), fileName);
            Table table = Table.fromSource(content, schema, getCsvFormat());
            tables.add(table);
        }
        return tables;
    }
    private List<Table> readfromOrdinaryFile() throws IOException {
        List<Table> tables = new ArrayList<>();
        for (File file : paths) {
                /* from the spec: "SECURITY: / (absolute path) and ../ (relative parent path)
                   are forbidden to avoid security vulnerabilities when implementing data
                   package software."

                   https://frictionlessdata.io/specs/data-resource/index.html#url-or-path
                 */
            Path securePath = Resource.toSecure(file.toPath(), basePath.toPath());
            Path relativePath = basePath.toPath().relativize(securePath);
            Table table = createTable(relativePath.toFile());
            tables.add(table);
        }
        return tables;
    }

    @JsonIgnore
    public void setIsInArchive(boolean isInArchive) {
        this.isInArchive = isInArchive;
    }
}
