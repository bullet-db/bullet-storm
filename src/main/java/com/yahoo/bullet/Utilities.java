package com.yahoo.bullet;

import java.util.Collection;
import java.util.Map;

public class Utilities {
    /**
     * Tries to get a key from a map as the target type.
     *
     * @param map  The non-null map that possibly contains the key.
     * @param key  The String key to get the value for from the map.
     * @param <U> The type to get the object as.
     * @return The casted object of type U or null.
     * @throws ClassCastException if the cast could not be done.
     * @throws NullPointerException if the map was null.
     */
    @SuppressWarnings("unchecked")
    public static <U> U getCasted(Map<String, Object> map, String key) {
        Object item = map.get(key);
        if (item == null) {
            return null;
        }
        return (U) item;
    }

    /**
     * Checks to see if the {@link Map} contains any mappings.
     *
     * @param map The map to check.
     * @return A boolean denoting whether the map had mappings.
     */
    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    /**
     * Checks to see if the {@link Collection} contains any items.
     *
     * @param collection The collection to check.
     * @return A boolean denoting whether the collection had items.
     */
    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }
}
