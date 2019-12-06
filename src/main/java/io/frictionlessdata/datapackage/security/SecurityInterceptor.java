package io.frictionlessdata.datapackage.security;

/**
 * Base interface for security interceptors called during Resource loading
 * @param <T> the type of reference the interceptor should handle.
 */
public interface SecurityInterceptor<T> {

    /**
     * Check the reference against the interceptor's rules to see whether
     * the reference might pose a security risk. Resource loading code
     * will reject references that violate those rules.
     *
     * @param reference the reference to check
     * @return true if the reference is acceptable, false if it MUST NOT be parsed
     */
    boolean accept(T reference);
}
