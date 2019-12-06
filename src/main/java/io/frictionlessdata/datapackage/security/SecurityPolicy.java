package io.frictionlessdata.datapackage.security;

import java.util.*;

public abstract class SecurityPolicy {
    private Map<Class, SecurityInterceptor> interceptorMap = new HashMap<>();

    public void addInterceptor(SecurityInterceptor ic, Class clazz) {
        interceptorMap.put(clazz, ic);
    }

    Collection filter(Collection references) {
        List retVal = new ArrayList();
        for (Object ref : references) {
            if (accept(ref)) {
                retVal.add(ref);
            }
        }
        return retVal;
    }

    boolean accept(Object ref) {
        Class clazz = ref.getClass();
        SecurityInterceptor ic = interceptorMap.get(clazz);
        if ((null != ic) && (ic.accept(ref))) {
            return true;
        }
        return false;
    }

}
