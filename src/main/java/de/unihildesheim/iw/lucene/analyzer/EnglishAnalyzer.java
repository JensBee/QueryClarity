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

package de.unihildesheim.iw.lucene.analyzer;

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
  final Version matchVersion;

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param v Lucene version to match
   * @param newStopwords stop words
   */
  public EnglishAnalyzer(final Version v,
      final CharArraySet newStopwords) {
    super(newStopwords);
    this.matchVersion = v;
  }

  /**
   * Builds an analyzer with the default Lucene version and stopwords from the
   * given {@link IndexDataProvider}.
   */
  public EnglishAnalyzer(final IndexDataProvider dataProv) {
    super(new CharArraySet(dataProv.getStopwords(), true));
    this.matchVersion = LuceneDefaults.VERSION;
  }

  /**
   * This configuration must match with the configuration used for the index!
   *
   * @param fieldName Document field
   * @param reader Index Reader
   * @return Token stream
   */
  @Override
  public final TokenStreamComponents createComponents(final String fieldName,
      final Reader reader) {
    final StandardTokenizer src = new StandardTokenizer(reader);
    TokenStream tok = new StandardFilter(src);
    tok = new EnglishPossessiveFilter(this.matchVersion, tok);
    tok = new LowerCaseFilter(tok);
//    tok = new WordDelimiterFilter(tok,
//        WordDelimiterFilter.GENERATE_NUMBER_PARTS |
//            WordDelimiterFilter.GENERATE_WORD_PARTS |
//            WordDelimiterFilter.SPLIT_ON_NUMERICS |
//            WordDelimiterFilter.SPLIT_ON_CASE_CHANGE,
//        null
//    );
    tok = new StopFilter(tok, getStopwordSet());
    tok = new PorterStemFilter(tok);
    return new TokenStreamComponents(src, tok);
  }
}
