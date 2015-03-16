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

package de.unihildesheim.iw.cli;

import de.unihildesheim.iw.Buildable.BuildException;
import de.unihildesheim.iw.Buildable.ConfigurationException;
import de.unihildesheim.iw.lucene.index.AbstractIndexDataProviderBuilder.Feature;
import de.unihildesheim.iw.lucene.index.FDRIndexDataProvider;
import de.unihildesheim.iw.lucene.index.FilteredDirectoryReader;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.LuceneIndexDataProvider.Builder;
import de.unihildesheim.iw.lucene.index.TermFilter.CommonTerms;
import de.unihildesheim.iw.util.TimeMeasure;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class FDRTest {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(FDRTest.class);

  private FDRTest(final String idxDirStr)
      throws IOException, ConfigurationException, BuildException {
    final File idxDir = new File(idxDirStr);
    final FSDirectory luceneDir = FSDirectory.open(idxDir.toPath());
    if (!DirectoryReader.indexExists(luceneDir)) {
      throw new IOException("No index found at index path '" + idxDir
          .getCanonicalPath() + "'.");
    }
    final DirectoryReader foo = DirectoryReader.open(luceneDir);

    final String qTerm = "abwasserreinigungsstuf";
    final String qField = "detd";
    final Set<String> qFields = Collections.singleton(qField);

    final Query q = new TermQuery(new Term(qField, qTerm));
    final Filter f = new QueryWrapperFilter(q);
    final IndexReader fooWrapped = new FilteredDirectoryReader
        .Builder(foo)
        .fields(qFields)
        .queryFilter(f)
        .termFilter(new CommonTerms(0.01))
        .build();

    LOG.debug("leaves: {}", fooWrapped.leaves().size());
    LOG.debug("qTerm: {}", qTerm);
    LOG.debug("qField: {}", qField);
    LOG.debug("Q: {}", q);

    LOG.debug("O|MaxDoc: {}", foo.maxDoc());
    LOG.debug("O|NumDocs: {}", foo.numDocs());
    LOG.debug("O|DocCount: f(claims)={} f(detd)={}",
        foo.getDocCount("claims"), foo.getDocCount("detd"));

    LOG.debug("W|MaxDoc: {}", fooWrapped.maxDoc());
    LOG.debug("W|NumDocs: {}", fooWrapped.numDocs());
    LOG.debug("W|DocCount: f(claims)={} f(detd)={}",
        fooWrapped.getDocCount("claims"), fooWrapped.getDocCount("detd"));

    final BytesRef sTerm = new BytesRef(qTerm.getBytes(StandardCharsets.UTF_8));

    Terms terms;
    TermsEnum termsEnum;
    boolean seekSucceed;

    terms = MultiFields.getTerms(foo, qField);
    if (terms == null) {
      LOG.debug("O| field={} no terms (null)", qField);
    } else {
      termsEnum = terms.iterator(TermsEnum.EMPTY);
      seekSucceed = termsEnum.seekExact(sTerm);
      if (seekSucceed) {
        LOG.debug("O|Seek to sTerm. df={}", termsEnum.docFreq());
      } else {
        LOG.error("O|Seek to sTerm failed.");
      }
    }

    terms = MultiFields.getTerms(fooWrapped, qField);
    if (terms == null) {
      LOG.debug("W| field={} no terms (null)", qField);
    } else {
      termsEnum = terms.iterator(TermsEnum.EMPTY);
      seekSucceed = termsEnum.seekExact(sTerm);
      if (seekSucceed) {
        LOG.debug("W|Seek to sTerm. df={}", termsEnum.docFreq());
      } else {
        LOG.error("W|Seek to sTerm failed.");
      }
    }

    DocsEnum de;
    int docs = 0;
    de = MultiFields.getTermDocsEnum(foo, MultiFields.getLiveDocs(foo),
        qField, new BytesRef(qTerm.getBytes(StandardCharsets.UTF_8)));
    while (true) {
      final int doc = de.nextDoc();
      if (doc == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      }
      docs++;
    }
    LOG.debug("O| docs f={} t={} c={}", qField, qTerm, docs);

    de = MultiFields.getTermDocsEnum(fooWrapped,
        MultiFields.getLiveDocs(fooWrapped), qField,
        new BytesRef(qTerm.getBytes(StandardCharsets.UTF_8)));
    docs = 0;
    if (de != null) {
      while (true) {
        final int doc = de.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        docs++;
      }
    }
    LOG.debug("W| docs f={} t={} c={}", qField, qTerm, docs);

    LOG.debug("O| init DataProvider");
    final IndexDataProvider oDataProv = new Builder()
        .indexReader(foo)
        .documentFields(qFields)
        .setFeature(Feature.COMMON_TERM_THRESHOLD, "0.01")
        .build();
    LOG.debug("O| init DataProvider done");
    LOG.debug("O| docIds={}", oDataProv.getDocumentIds().count());

    LOG.debug("W| init DataProvider");
    final IndexDataProvider wDataProv = new Builder()
        .indexReader(fooWrapped)
        .documentFields(qFields)
        .build();
    LOG.debug("W| init DataProvider done");
    LOG.debug("W| docIds={}", wDataProv.getDocumentIds().count());

    final TimeMeasure t = new TimeMeasure().start();
    LOG.debug("N| init DataProvider");
    final IndexDataProvider nDataProv = new FDRIndexDataProvider.Builder()
        .indexReader(fooWrapped)
        .build();
    LOG.debug("N| init DataProvider done");
    LOG.debug("Time taken {}", t.stop().getTimeString());
    LOG.debug("N| docIds={}", nDataProv.getDocumentIds().count());
  }

  public static void main(final String[] args)
      throws IOException, BuildException, ConfigurationException {
    if (args.length <= 0) {
      LOG.error("No index dir specified.");
      System.exit(-1);
    }
    new FDRTest(args[0]);
  }
}
