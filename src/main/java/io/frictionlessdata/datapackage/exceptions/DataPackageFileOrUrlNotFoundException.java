package io.frictionlessdata.datapackage.exceptions;

public class DataPackageFileOrUrlNotFoundException extends DataPackageException {
    public DataPackageFileOrUrlNotFoundException() {
    }

    public DataPackageFileOrUrlNotFoundException(String msg) {
        super(msg);
    }

    public DataPackageFileOrUrlNotFoundException(Throwable t) {
        super(t);
    }
}
