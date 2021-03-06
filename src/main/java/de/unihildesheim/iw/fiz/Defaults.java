/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

package de.unihildesheim.iw.fiz;

/**
 * Default values for interacting with the ES based document repository and the
 * scoring tools.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class Defaults {
//  /**
//   * Languages provided by ES index.
//   */
//  public enum SRC_LANGUAGE {
//    /**
//     * German.
//     */
//    DE,
//    /**
//     * English
//     */
//    EN,
//    /**
//     * French
//     */
//    FR;
//
//    /**
//     * Try to get a language by it's name.
//     *
//     * @param lng Language identifier as string
//     * @return Language or {@code null} if none was found for the given string
//     */
//    @Nullable
//    public static SRC_LANGUAGE getByString(final String lng) {
//      for (final SRC_LANGUAGE srcLng : SRC_LANGUAGE.values()) {
//        if (lng.equalsIgnoreCase(srcLng.name())) {
//          return srcLng;
//        }
//      }
//      return null;
//    }
//  }

  /**
   * ES settings. (TODO: make these external)
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class ES_CONF {
    // basic settings
    /**
     * Name of the index to query.
     */
    public static final String INDEX = "epfull_repo";
    /**
     * URL to reach the index.
     */
    public static final String URL = "http://t4p.fiz-karlsruhe.de:80";
    /**
     * Type of document to query for.
     */
    public static final String DOC_TYPE = "patent";
    /**
     * Number of results to get from each shard.
     */
    public static final int PAGE_SIZE = 500;
    /**
     * How long to keep the scroll open.
     */
    public static final String SCROLL_KEEP = "15m";

    // document fields
    /**
     * Field name containing the document id.
     */
    public static final String FLD_DOCID = "_id";
    /**
     * Prefix name of the claims field.
     */
    public static final String FLD_CLAIM_PREFIX = "CLM";
    /**
     * Field name holding the description.
     */
    public static final String FLD_DESC = "DETD";
    /**
     * Field name holding the description language used.
     */
    public static final String FLD_DESC_LNG = "DETDL";
    /**
     * Field holding a reference to the patent.
     */
    public static final String FLD_PATREF = "PN";
    /**
     * Field holding IPC codes.
     */
    public static final String FLD_IPC = "IPC";

    /**
     * How many times to retry a connection.
     */
    public static final int MAX_RETRY = 100;
  }
}
