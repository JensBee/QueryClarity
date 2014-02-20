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
package de.unihildesheim.lucene.scoring.clarity;

import de.unihildesheim.lucene.scoring.Scoring;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import de.unihildesheim.lucene.LuceneDefaults;
import de.unihildesheim.lucene.document.DocumentModelException;
import de.unihildesheim.lucene.index.CachedIndexDataProvider;
import de.unihildesheim.util.concurrent.processing.Processing;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class Main {

  /**
   * CLI-parameter to specify the query to run.
   */
  @Parameter(names = "-query", description = "Query string", required = true)
  private String queryString;
  /**
   * CLI-parameter to specify the Lucene index directory.
   */
  @Parameter(names = "-index", description = "Lucene index", required = true)
  private String indexDir;

  /**
   * Private constructor for utility class.
   */
  private Main() {
    // empty constructor for utility class
  }

  /**
   * Private constructor for static main class.
   *
   * @throws IOException If index could not be read
   * @throws org.apache.lucene.queryparser.classic.ParseException If the
   * {@link Query} could not be parsed
   * @throws DocumentModelException If a requested document model needed for
   * calculation could not be created
   */
  private void runMain() throws IOException, DocumentModelException,
          ParseException {
    LOG.debug("Starting");

    if (this.indexDir.isEmpty() || this.queryString.isEmpty()) {
      LOG.error("No index or query specified.");
      Runtime.getRuntime().exit(1);
    }

    // index field to operate on
    final String[] fields = new String[]{"text"};

    // open index
    final Directory directory = FSDirectory.open(new File(this.indexDir));

    try (IndexReader reader = DirectoryReader.open(directory)) {
      final String storageId = "clef";
      final CachedIndexDataProvider dataProv = new CachedIndexDataProvider(
              storageId);
      if (!dataProv.tryGetStoredData()) {
        dataProv.recalculateData(reader, fields, false);
      }

      final Scoring calculation = new Scoring(dataProv, reader);

      final Analyzer analyzer = new StandardAnalyzer(LuceneDefaults.VERSION);

      LOG.debug("Building query ({}).", queryString);
      final QueryParser parser = new QueryParser(LuceneDefaults.VERSION,
              fields[0], analyzer);
      final Query query = parser.parse(queryString);

      final ClarityScoreCalculation csCalc = calculation.newInstance(
              Scoring.ClarityScore.DEFAULT);
      csCalc.calculateClarity(query);

      LOG.debug("Closing lucene index.");
      dataProv.dispose();
    }

    Processing.shutDown();
    LOG.debug("Finished");
  }

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  /**
   * @param args Command line parameters
   * @throws IOException If index could not be read
   * @throws org.apache.lucene.queryparser.classic.ParseException If the
   * {@link Query} could not be parsed
   * @throws DocumentModelException If a requested document model needed for
   * calculation could not be created
   */
  public static void main(final String[] args) throws IOException,
          DocumentModelException, ParseException {
    final Main main = new Main();
    final JCommander jc = new JCommander(main);
    try {
      jc.parse(args);
    } catch (ParameterException ex) {
      System.out.println(ex.getMessage() + "\n");
      jc.usage();
      System.exit(1);
    }
    main.runMain();
  }
}
