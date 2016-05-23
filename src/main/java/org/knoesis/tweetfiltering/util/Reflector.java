package org.knoesis.tweetfiltering.util;

import java.lang.reflect.InvocationTargetException;

import org.knoesis.tweetfiltering.util.Reflector;

public class Reflector {
    
    private final Object INSTANCE;
    
    private Reflector(Object instance) {
        this.INSTANCE = instance;
    }
    
    public static Reflector create(String className, Object[] args, Class... assignables) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
        Class clazz = Class.forName(className);
        for (Class assignable : assignables) {
            if (!assignable.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(clazz.getName() + " is not a " + assignable.getName());
            }
        }
        Class[] argTypes = new Class[args.length];
        for (int i=0; i<argTypes.length; i++) {
            argTypes[i] = args.getClass();
        }
        return new Reflector(clazz.getConstructor(argTypes).newInstance(args));
    }
    
    public <T> T get() {
        return (T) INSTANCE;
    }
    
}