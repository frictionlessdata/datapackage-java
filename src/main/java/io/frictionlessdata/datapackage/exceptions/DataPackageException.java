package io.frictionlessdata.datapackage.exceptions;

/**
 *
 *
 */
public class DataPackageException extends RuntimeException {

    /**
     * Creates a new instance of <code>DataPackageException</code> without
     * detail message.
     */
    public DataPackageException() {
    }

    /**
     * Constructs an instance of <code>DataPackageException</code> with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    public DataPackageException(String msg) {
        super(msg);
    }

    /**
     * Constructs an instance of <code>DataPackageException</code> by wrapping a Throwable
     *
     * @param t the wrapped exception.
     */
    public DataPackageException(String msg, Throwable t) {
        super(msg, t);
    }

    /**
     * Constructs an instance of <code>DataPackageException</code> by wrapping a Throwable
     *
     * @param t the wrapped exception.
     */
    public DataPackageException(Throwable t) {
        super(t.getMessage(), t);
    }
}