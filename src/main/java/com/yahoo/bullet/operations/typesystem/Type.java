/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.typesystem;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Getter
public enum Type {
    STRING(String.class),
    BOOLEAN(Boolean.class),
    LONG(Long.class),
    DOUBLE(Double.class),
    LIST(List.class),
    MAP(Map.class),
    // Doesn't matter what underlyingType is for null, just need something that isn't encountered
    NULL(Type.class);

    public static final String NULL_EXPRESSION = "null";
    /**
     * Only support the atomic types for now since all our operations are on atomic types.
     */
    public static List<Type> SUPPORTED_TYPES = Type.simpleTypes();
    private final Class underlyingType;

    /**
     * Constructor.
     *
     * @param underlyingType The Java type that this type represents.
     */
    Type(Class underlyingType) {
        this.underlyingType = underlyingType;
    }

    /**
     * The atomic, simple, non-null types we support.
     *
     * @return The list of simple types.
     */
    public static List<Type> simpleTypes() {
        return Arrays.asList(BOOLEAN, LONG, DOUBLE, STRING);
    }

    /**
     * Tries to get the type of a given object from {@link #SUPPORTED_TYPES}.
     *
     * @param object The object whose type is to be determined.
     * @return {@link Type} for this object, the {@link Type#NULL} if the object was null or null
     *         if the type could not be determined.
     */
    public static Type getType(Object object) {
        if (object == null) {
            return Type.NULL;
        }
        for (Type type : SUPPORTED_TYPES) {
            if (type.getUnderlyingType().isInstance(object)) {
                return type;
            }
        }
        // Unknown type
        return null;
    }

    /**
     * Takes a value and casts it to this type.
     *
     * @param value The string value that is being cast.
     * @return The casted object.
     * @throws RuntimeException if the cast cannot be done.
     */
    public Object cast(String value) {
        switch (this) {
            case BOOLEAN:
                return Boolean.valueOf(value);
            case LONG:
                // If we want to allow decimals to be casted as longs, do Double.valueOf(value).longValue() instead
                return Long.valueOf(value);
            case DOUBLE:
                return Double.valueOf(value);
            case STRING:
                return value;
            case NULL:
                return NULL_EXPRESSION.compareToIgnoreCase(value) == 0 ? null : value;
            // We won't support the rest for castability
            default:
                throw new ClassCastException("Cannot cast " + value + " to type " + this);
        }
    }

}

