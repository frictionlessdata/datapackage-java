package io.frictionlessdata.datapackage.exceptions;

/**
 *
 *
 */
public class DataPackageValidationException extends DataPackageException {

    /**
     * Creates a new instance of <code>DataPackageException</code> without
     * detail message.
     */
    public DataPackageValidationException() {
    }

    /**
     * Constructs an instance of <code>DataPackageException</code> with the
     * specified detail message.
     *
     * @param msg the detail message.
     */
    public DataPackageValidationException(String msg) {
        super(msg);
    }

    /**
     * Constructs an instance of <code>DataPackageException</code> by wrapping a Throwable
     *
     * @param t the wrapped exception.
     */
    public DataPackageValidationException(Throwable t) {
        super(t.getMessage(), t);
    }
    /**
     * Constructs an instance of <code>DataPackageException</code> by wrapping a Throwable
     *
     * @param t the wrapped exception.
     */
    public DataPackageValidationException(String msg, Throwable t) {
        super(msg, t);
    }
}