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

package de.unihildesheim.iw.lucene.index.builder;

import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.lucene.VecTextField;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers;
import de.unihildesheim.iw.lucene.analyzer.LanguageBasedAnalyzers.Language;
import de.unihildesheim.iw.lucene.index.builder.PatentDocument.RequiredFields;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class IndexBuilder
    implements AutoCloseable {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      org.slf4j.LoggerFactory.getLogger(IndexBuilder.class);
  private final Language language;
  private final IndexWriter writer;

  public IndexBuilder(
      final Path target,
      final Language lang, final Set<String> stopwords)
      throws IOException {
    // check, if we've an analyzer for the current language
    if (!LanguageBasedAnalyzers.hasAnalyzer(lang.toString())) {
      throw new IllegalArgumentException(
          "No analyzer for language '" + lang + "'.");
    }

    // get an analyzer for the target language
    final Analyzer analyzer = LanguageBasedAnalyzers.createInstance
        (LanguageBasedAnalyzers.getLanguage(lang.toString()),
            LuceneDefaults.VERSION, new CharArraySet(stopwords, true));

    // Lucene index setup
    final Directory index = FSDirectory.open(target);
    final IndexWriterConfig config = new IndexWriterConfig(analyzer);

    this.writer = new IndexWriter(index, config);
    this.language = lang;
  }

  public void index(final PatentDocument patent)
      throws IOException {
    // create Lucene document from model
    final Document patDoc = new Document();
    boolean hasData = false;

    if (!patent.hasField(RequiredFields.P_ID, null)) {
      LOG.warn("Patent reference was empty! Skipping document.");
      return;
    }

    // test, if we have claim data
    if (patent.hasField(RequiredFields.CLAIMS, this.language)) {
      patDoc.add(new VecTextField(LUCENE_CONF.FLD_CLAIMS,
          Objects.requireNonNull(
              patent.getField(RequiredFields.CLAIMS, this.language)),
          Store.NO));
      hasData = true;
    }

    // test, if we have detailed description data
    if (patent.hasField(RequiredFields.DETD, this.language)) {
      patDoc.add(new VecTextField(LUCENE_CONF.FLD_DETD,
          patent.getField(RequiredFields.DETD, this.language), Store.NO));
      hasData = true;
    }

    // check if there's something to index
    if (hasData) {
      // add patent-id
      patDoc.add(new StringField(LUCENE_CONF.FLD_PAT_ID,
          patent.getField(RequiredFields.P_ID, null), Store.YES));

      // test, if we can add ipcs
      if (patent.hasField(RequiredFields.IPC, null)) {
        patDoc.add(new VecTextField(LUCENE_CONF.FLD_IPC,
            patent.getField(RequiredFields.IPC, null), Store.YES));
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("Add pat:{} [claims:{} detd:{} ipc:{}]",
            patent.getField(RequiredFields.P_ID, null),
            patent.hasField(RequiredFields.CLAIMS, this.language),
            patent.hasField(RequiredFields.DETD, this.language),
            patent.getField(RequiredFields.IPC, null));
      }
      this.writer.addDocument(patDoc);
    } else {
      LOG.warn("No data to write. Skipping document.");
    }
  }

  @Override
  public void close()
      throws Exception {
    this.writer.close();
  }

  /**
   * Default Lucene index configuration.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class LUCENE_CONF {
    /**
     * Field containing a unique patent identifier.
     */
    public static final String FLD_PAT_ID = "pat_id";
    /**
     * Field containing claims.
     */
    public static final String FLD_CLAIMS = "claims";
    /**
     * Field containing detailed description.
     */
    public static final String FLD_DETD = "detd";
    /**
     * Field containing ipcs.
     */
    public static final String FLD_IPC = "ipc";
  }
}
