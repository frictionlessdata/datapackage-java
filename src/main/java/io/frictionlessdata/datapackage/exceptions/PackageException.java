package io.frictionlessdata.datapackage.exceptions;

/**
 *
 *
 */
public class PackageException extends Exception {

    /**
     * Creates a new instance of <code>DataPackageException</code> without
     * detail message.
     */
    public PackageException() {
    }

    /**
     * Constructs an instance of <code>DataPackageException</code> with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    public PackageException(String msg) {
        super(msg);
    }
}