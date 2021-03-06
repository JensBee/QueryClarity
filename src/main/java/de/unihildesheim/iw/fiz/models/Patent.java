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
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers.Language;
import de.unihildesheim.iw.lucene.index.builder.PatentDocument;
import de.unihildesheim.iw.util.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class Patent
    implements PatentDocument {
  /**
   * Some special chars that will be replaced before indexing.
   */
  @SuppressWarnings("HardcodedLineSeparator")
  private static final Pattern RX_SPECIAL_CHARS =
      Pattern.compile("\\n|\\\\n|\\r|\\\\r|\\t|\\\\t");
  /**
   * Fold multiple spaces into a single space character.
   */
  private static final Pattern RX_SPACE_FOLD = Pattern.compile("/\\s+/");
  /**
   * Claims by language.
   */
  @Nullable
  private Map<Language, String> claimsByLanguage;
  /**
   * Detailed descriptions by language.
   */
  @Nullable
  private Map<Language, String> detdByLanguage;
  /**
   * List of ipcs.
   */
  @Nullable
  private Set<String> ipcs;
  /**
   * Patent identifier.
   */
  @Nullable
  private String patId;
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(Patent.class);

  public static PatentDocument fromJson(@NotNull final JsonObject json) {
    Objects.requireNonNull(json);
    final Patent p = new Patent();

    if (json.has("fields")) {
      final JsonObject hitFieldsJson = json.getAsJsonObject("fields");

      // collect claims
      p.claimsByLanguage = Arrays.stream(Language.values())
          .filter(l -> hitFieldsJson.has(
              ES_CONF.FLD_CLAIM_PREFIX + l.toUpperCaseString()))
          .collect(
              HashMap<Language, String>::new,
              (map, l) -> map.put(l,
                  StreamSupport.stream(hitFieldsJson
                      .getAsJsonArray(
                          ES_CONF.FLD_CLAIM_PREFIX + l.toUpperCaseString())
                      .spliterator(), false)
                      .map(c ->
                          RX_SPACE_FOLD.matcher(
                              RX_SPECIAL_CHARS.matcher(c.getAsString())
                                  .replaceAll(" ")).replaceAll(" "))
                      .collect(Collectors.joining(" "))),
              HashMap<Language, String>::putAll);

      // collect detd
      if (hitFieldsJson.has(ES_CONF.FLD_DESC_LNG) &&
          hitFieldsJson.has(ES_CONF.FLD_DESC)) {
        final String detdl = hitFieldsJson.getAsJsonArray(
            ES_CONF.FLD_DESC_LNG).get(0).getAsString();
        p.detdByLanguage = Arrays.stream(Language.values())
            .filter(l -> l.toString().equalsIgnoreCase(detdl))
            .collect(HashMap<Language, String>::new,
                (map, l) -> map.put(l, joinJsonArray(
                    hitFieldsJson.getAsJsonArray(ES_CONF.FLD_DESC))),
                HashMap<Language, String>::putAll);
      } else {
        p.detdByLanguage = Collections.emptyMap();
      }

      // collect IPC(s)
      if (hitFieldsJson.has(ES_CONF.FLD_IPC)) {
        p.ipcs = StreamSupport.stream(
            hitFieldsJson.getAsJsonArray(ES_CONF.FLD_IPC).spliterator(), false)
            .map(JsonElement::getAsString)
            .collect(Collectors.toSet());
      } else {
        p.ipcs = Collections.emptySet();
      }

      // construct model
      p.patId = hitFieldsJson.get(ES_CONF.FLD_PATREF).getAsString();
    }
    return p;
  }

  @SuppressWarnings("TypeMayBeWeakened")
  public static String joinJsonArray(@NotNull final JsonArray jArr) {
    return StreamSupport.stream(jArr.spliterator(), false)
        .map(e -> RX_SPACE_FOLD.matcher(
            RX_SPECIAL_CHARS.matcher(e.getAsString()
            ).replaceAll(" ")).replaceAll(" "))
        .collect(Collectors.joining(" "));
  }

  @Override
  @Nullable
  public String getField(
      @NotNull final RequiredFields fld,
      @Nullable final Language lng) {
    switch (fld) {
      case P_ID:
        if (this.patId == null) {
          return "";
        }
        return this.patId;
      case CLAIMS:
        if (lng == null || this.claimsByLanguage == null) {
          return "";
        }
        return this.claimsByLanguage.get(lng);
      case DETD:
        if (lng == null || this.detdByLanguage == null) {
          return "";
        }
        return this.detdByLanguage.get(lng);
      case IPC:
        if (this.ipcs == null) {
          return "";
        }
        return StringUtils.join(this.ipcs, " ");
    }
    return "";
  }

  @Override
  public boolean hasField(
      @NotNull final RequiredFields fld,
      @Nullable final Language lng) {
    switch (fld) {
      case P_ID:
        return this.patId != null && !this.patId.isEmpty();
      case CLAIMS:
        return lng != null &&
            this.claimsByLanguage != null &&
            this.claimsByLanguage.get(lng) != null;
      case DETD:
        return lng != null &&
            this.detdByLanguage != null &&
            this.detdByLanguage.get(lng) != null;
      case IPC:
        return this.ipcs != null && !this.ipcs.isEmpty();
    }
    return false;
  }
}
