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
package de.unihildesheim.lucene.queryclarity;

import de.unihildesheim.lucene.queryclarity.indexdata.IndexDataException;
import de.unihildesheim.lucene.queryclarity.indexdata.DefaultIndexDataProvider;
import de.unihildesheim.lucene.queryclarity.indexdata.AbstractIndexDataProvider;
import java.io.File;
import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class Main {

  /**
   * Private constructor for static main class.
   */
  private Main() {

  }

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);
  /**
   * Lucene field to run the queries on.
   */
  private static final String QUERY_FIELD = "text";

  /**
   * @param args Command line parameters
   * @throws IOException If index could not be read
   * @throws org.apache.lucene.queryparser.classic.ParseException
   * @throws de.unihildesheim.lucene.queryclarity.indexData.IndexDataException
   * Thrown, if not all requested fields are present in the index
   */
  public static void main(final String[] args) throws IOException,
          ParseException, IndexDataException {
    String index = null;
    String queryString = "";
    final String usage = "Usage:\t" + Main.class.getCanonicalName()
            + " -index <dir> -query <query>.";

    LOG.debug("Starting");
    if (args.length == 0 || (args.length > 0 && ("-h".equals(args[0])
            || "-help".equals(args[0])))) {
      LOG.info(usage);
      System.exit(0);
    }

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "-index":
          index = args[i + 1];
          i++;
          break;
        case "-query":
          queryString = args[i + 1];
          i++;
          break;
      }
    }

    // index field to operate on
    final String[] fields = new String[]{"text"};

    // open index
    final Directory directory = FSDirectory.open(new File(index));
    final IndexReader reader = DirectoryReader.open(directory);

    // create data provider instance
    AbstractIndexDataProvider dataProv = new DefaultIndexDataProvider(reader,
            fields);

    final Calculation calculation = new Calculation(dataProv, reader, fields);

    final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);

    LOG.debug("Building query ({}).", queryString);
    final QueryParser parser = new QueryParser(Version.LUCENE_46, QUERY_FIELD,
            analyzer);
    final Query query = parser.parse(queryString);

    calculation.calculateClarity(query);

    LOG.debug("Closing lucene index ({}).", index);
    calculation.dispose();

    LOG.debug("Finished");
  }
}
