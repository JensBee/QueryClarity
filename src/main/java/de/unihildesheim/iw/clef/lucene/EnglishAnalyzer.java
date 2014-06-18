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

package de.unihildesheim.iw.clef.lucene;

import de.unihildesheim.iw.lucene.LuceneDefaults;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

import java.io.Reader;

/**
 * @author Jens Bertram
 */
public final class EnglishAnalyzer
    extends StopwordAnalyzerBase {

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param version Lucene version to match
   * @param newStopwords stop words
   */
  public EnglishAnalyzer(final Version version,
      final CharArraySet newStopwords) {
    super(version, newStopwords);
  }

  /**
   * Builds an analyzer with the default Lucene version and stopwords from the
   * given {@link IndexDataProvider}.
   */
  public EnglishAnalyzer(final IndexDataProvider dataProv) {
    super(LuceneDefaults.VERSION, new CharArraySet(LuceneDefaults.VERSION,
        dataProv.getStopwords(), true));
  }

  /**
   * This configuration must match with the configuration used for the index!
   *
   * @param fieldName Document field
   * @param reader Index Reader
   * @return Token stream
   */
  @Override
  protected final TokenStreamComponents createComponents(final String fieldName,
      final Reader reader) {
    final StandardTokenizer src = new StandardTokenizer(
        this.matchVersion, reader);
    TokenStream tok = new StandardFilter(this.matchVersion, src);
    tok = new EnglishPossessiveFilter(this.matchVersion, tok);
    tok = new LowerCaseFilter(this.matchVersion, tok);
    tok = new StopFilter(this.matchVersion, tok, getStopwordSet());
    /*
    tok = new WordDelimiterFilter(tok,
        WordDelimiterFilter.GENERATE_NUMBER_PARTS |
            WordDelimiterFilter.GENERATE_WORD_PARTS |
            WordDelimiterFilter.SPLIT_ON_CASE_CHANGE, null
    );
    */
    tok = new PorterStemFilter(tok);
    return new TokenStreamComponents(src, tok);
  }
}
