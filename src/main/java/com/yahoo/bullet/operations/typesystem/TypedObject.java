/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations.typesystem;

import lombok.Getter;

import java.util.Objects;

@Getter
public class TypedObject implements Comparable<TypedObject> {
    private final Type type;
    private final Object value;

    /**
     * Constructor that forces the type and value to string if type is not known.
     *
     * @param value The value who is being wrapped.
     */
    public TypedObject(Object value) {
        Type type = Type.getType(value);
        // Unknown type -> force to string
        if (type == null) {
            type = Type.STRING;
            value = value.toString();
        }
        this.type = type;
        this.value = value;
    }

    /**
     * For testing purposes mostly. Create a TypedObject with the given non-null type.
     *
     * @param type The type of the value.
     * @param value The payload.
     */
    public TypedObject(Type type, Object value) {
        Objects.requireNonNull(type);
        this.type = type;
        this.value = value;
    }

    /**
     * Takes a value and a returns a casted TypedObject according to this type.
     *
     * @param value The string value that is being cast.
     * @return The casted TypedObject or null if the cast cannot be done.
     */
    public TypedObject typeCast(String value) {
        try {
            return new TypedObject(this.type, this.type.cast(value));
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Compares this TypedObject to another. If the value is null and the other isn't,
     * returns Integer.MIN_VALUE.
     * {@inheritDoc}
     *
     * @param o The other non-null TypedObject
     * @return {@inheritDoc}
     */
    @Override
    public int compareTo(TypedObject o) {
        Objects.requireNonNull(o);
        // If type casting/unification needs to happen, it should go here. Assume this.type == o.type for now
        switch (type) {
            case STRING:
                return value.toString().compareTo((String) o.value);
            case BOOLEAN:
                return ((Boolean) value).compareTo((Boolean) o.value);
            case LONG:
                return ((Long) value).compareTo((Long) o.value);
            case DOUBLE:
                return ((Double) value).compareTo((Double) o.value);
            case NULL:
                // Return Integer.MIN_VALUE if the type isn't null. We could throw an exception instead.
                return o.value == null ? 0 : Integer.MIN_VALUE;
            default:
                throw new RuntimeException("Unsupported type cannot be compared: " + type);
        }
    }

    @Override
    public String toString() {
        return type == Type.NULL ? Type.NULL_EXPRESSION : value.toString();
    }
}

