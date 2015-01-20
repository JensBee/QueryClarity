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

package de.unihildesheim.iw.fiz.models;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.unihildesheim.iw.fiz.Defaults.ES_CONF;
import de.unihildesheim.iw.fiz.Defaults.SRC_LANGUAGE;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class Patent {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG = org.slf4j.LoggerFactory.getLogger(Patent.class);
  private static final String[] STRINGS = new String[]{};

  /** Claims by language. */
  private final Map<SRC_LANGUAGE, JsonArray> claimsByLanguage;
  /** Detailed descriptions by language. */
  private final Map<SRC_LANGUAGE, String> detdByLanguage;
  /** ES internal Document identifier. */
  private final String docId;
  /** Patent identifier. */
  private final String patId;

  public static Patent fromJson(JsonObject json) {
    Objects.requireNonNull(json);
    final Patent p;

    if (json.has("fields")) {
      final JsonObject hitFieldsJson = json.getAsJsonObject("fields");

      // collect claims
      final Map<SRC_LANGUAGE, JsonArray> claimsByLanguage =
          new EnumMap(SRC_LANGUAGE.class);
      for (final SRC_LANGUAGE lng : SRC_LANGUAGE.values()) {
        if (hitFieldsJson.has(ES_CONF.FLD_CLAIM_PREFIX + lng)) {
          claimsByLanguage.put(lng,
              hitFieldsJson.getAsJsonArray(ES_CONF.FLD_CLAIM_PREFIX + lng));
        }
      }

      // collect detd
      Map<SRC_LANGUAGE, String> detdByLanguage = null;
      if (hitFieldsJson.has(ES_CONF.FLD_DESC_LNG) &&
          hitFieldsJson.has(ES_CONF.FLD_DESC)) {
        final String detdl = hitFieldsJson.getAsJsonArray(
            ES_CONF.FLD_DESC_LNG).get(0).getAsString();
        for (final SRC_LANGUAGE lng : SRC_LANGUAGE.values()) {
          if (lng.toString().equalsIgnoreCase(detdl)) {
            detdByLanguage = Collections.singletonMap(lng,
                joinJsonArray(hitFieldsJson.getAsJsonArray(ES_CONF.FLD_DESC)));
            break;
          }
        }
      }
      if (detdByLanguage == null) {
        detdByLanguage = Collections.emptyMap();
      }

      // construct model
      p = new Patent(json.get(ES_CONF.FLD_DOCID).getAsString(),
          hitFieldsJson.get(ES_CONF.FLD_PATREF).getAsString(),
          claimsByLanguage, detdByLanguage);
    } else {
      // construct empty model
      p = new Patent(json.get(ES_CONF.FLD_DOCID).getAsString(), "");
    }
    return p;
  }

  private Patent(final String id, final String patId) {
    this.docId = id;
    this.patId = patId;
    this.claimsByLanguage = Collections.emptyMap();
    this.detdByLanguage = Collections.emptyMap();
  }

  private Patent(final String id, final String patId, final Map<SRC_LANGUAGE,
      JsonArray> claims,
      final Map<SRC_LANGUAGE, String> detd) {
    this.docId = id;
    this.patId = patId;
    this.claimsByLanguage = claims;
    this.detdByLanguage = detd;
  }

  /**
   * Get claims by language.
   * @param lng Language
   * @return Array of Strings or empty array if no claims were stored
   */
  public String[] getClaims(final SRC_LANGUAGE lng) {
    final JsonArray claims = this.claimsByLanguage.get(lng);
    if (claims == null) {
      return STRINGS;
    }
    final String[] claimsStrArr = new String[claims.size()];
    for (int i=0; i<claims.size(); i++) {
      claimsStrArr[i] = claims.get(i).getAsString();
    }
    return claimsStrArr;
  }

  private static String joinJsonArray(final JsonArray jArr) {
    final StringBuilder jStr = new StringBuilder();
    for (final JsonElement je : jArr) {
      jStr.append(je.getAsString());
    }
    return jStr.toString();
  }

  /**
   * Get all claims by language as single concatenated string.
   * @param lng Language
   * @return Claims combined by space character
   */
  public String getClaimsAsString(final SRC_LANGUAGE lng) {
    final String[] claimsStrArr = getClaims(lng);
    final StringBuilder claimsStr = new StringBuilder();
    for (final String claim : claimsStrArr) {
      claimsStr.append(claim).append(' ');
    }
    return claimsStr.substring(0, claimsStr.length() -1);
  }

  public String getDetd(final SRC_LANGUAGE lng) {
    return this.detdByLanguage.get(lng);
  }

  public boolean hasDetd(final SRC_LANGUAGE lng) {
    return this.detdByLanguage.get(lng) != null;
  }

  public boolean hasClaims(final SRC_LANGUAGE lng) {
    return this.claimsByLanguage.get(lng) != null;
  }

  /**
   * Get the document id.
   * @return Document id
   */
  public String getId() {
    return this.docId;
  }

  /**
   * Get the patent id.
   * @return Patent id
   */
  public String getPatId() {
    return this.patId;
  }
}
