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
import de.unihildesheim.iw.fiz.Defaults.SRC_LANGUAGE;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class Patent {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG = org.slf4j.LoggerFactory.getLogger(Patent.class);
  private static final String[] STRINGS = new String[]{};

  private final Map<SRC_LANGUAGE, JsonArray> claimsByLanguage;
  private final Map<SRC_LANGUAGE, String> detdByLanguage;
  private final String docId;

  public static Patent fromJson(JsonObject json) {
    if (json.has("fields")) {
      final JsonObject hitFieldsJson = json.getAsJsonObject("fields");

      // collect claims
      final Map<SRC_LANGUAGE, JsonArray> claimsByLanguage = new HashMap<>
          (SRC_LANGUAGE.values().length);
      for (SRC_LANGUAGE lng : SRC_LANGUAGE.values()) {
        if (hitFieldsJson.has("CLM" + lng)) {
          claimsByLanguage.put(lng, hitFieldsJson.getAsJsonArray("CLM" + lng));
        }
      }

      // collect detd
      Map<SRC_LANGUAGE, String> detdByLanguage = null;
      if (hitFieldsJson.has("DETDL") && hitFieldsJson.has("DETD")) {
        final String detdl = hitFieldsJson.getAsJsonArray("DETDL").get(0)
            .getAsString();
        for (SRC_LANGUAGE lng : SRC_LANGUAGE.values()) {
          if (lng.toString().equalsIgnoreCase(detdl)) {
            detdByLanguage = Collections.singletonMap(lng,
                joinJsonArray(hitFieldsJson.getAsJsonArray("DETD")));
            break;
          }
        }
      }
      if (detdByLanguage == null) {
        detdByLanguage = Collections.emptyMap();
      }

      // construct model
      return new Patent(json.get("_id").getAsString(), claimsByLanguage,
          detdByLanguage);
    }
    // construct empty model
    return new Patent(json.get("_id").getAsString());
  }

  private Patent(final String id) {
    this.docId = id;
    this.claimsByLanguage = Collections.emptyMap();
    this.detdByLanguage = Collections.emptyMap();
  }

  private Patent(final String id, final Map<SRC_LANGUAGE, JsonArray> claims,
      final Map<SRC_LANGUAGE, String> detd) {
    this.docId = id;
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
}
