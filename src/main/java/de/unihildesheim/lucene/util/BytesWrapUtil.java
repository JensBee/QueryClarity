/*
 * Copyright (C) 2014 Jens Bertram <code@jens-bertram.net>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.unihildesheim.lucene.util;

import java.util.WeakHashMap;
import org.apache.lucene.util.BytesRef;

/**
 * Utility functions to work with {@link BytesWrap} instances.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class BytesWrapUtil {

    private static final WeakHashMap<byte[], String> intern = new WeakHashMap<>(
            10000);

    /**
     * Private constructor for utility class.
     */
    private BytesWrapUtil() {
        // empty prvate constructor
    }

    /**
     * Interprets stored bytes as UTF8 bytes, returning the resulting string.
     * Actually a copy of {@link BytesRef#utf8ToString()}.
     *
     * @param bw BytesWrap instance whose byte array should be converted
     * @return String representation of the corresponding byte array
     */
    public static String bytesWrapToString(final BytesWrap bw) {
        String str = intern.get(bw.getBytes());
        if (str == null) {
            byte[] b = bw.getBytes();
            str = new BytesRef(b).utf8ToString();
            intern.put(b, str);
        }
        return str;
    }
}
