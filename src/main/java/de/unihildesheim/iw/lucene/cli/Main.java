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
package de.unihildesheim.iw.lucene.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import de.unihildesheim.iw.lucene.Environment;
import de.unihildesheim.iw.lucene.document.DocumentModelException;
import de.unihildesheim.iw.lucene.index.CachedIndexDataProvider;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Jens Bertram
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
   * CLI-parameter to specify the data directory.
   */
  @Parameter(names = "-data", description = "Data path", required = true)
  private String dataDir;

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
   * @throws org.apache.lucene.queryparser.classic.ParseException If the query
   * could not be parsed
   * @throws DocumentModelException If a requested document model needed for
   * calculation could not be created
   */
  private void runMain()
      throws Exception {
    LOG.debug("Starting");

    if (this.indexDir.isEmpty() || this.queryString.isEmpty()) {
      LOG.error("No index or query specified.");
      Runtime.getRuntime().exit(1);
    }

    final CachedIndexDataProvider cIdp = new CachedIndexDataProvider();
//    new Environment.Builder(indexDir, dataDir)
    new Environment.Builder(dataDir)
        .dataProvider(cIdp)
        .loadCache("testCache")
        .autoWarmUp()
        .build();
//    cIdp.cacheBuilder("testCache");

    // index field to operate on
//    final String[] fields = new String[]{"text"};
//    final String[] fields = new String[]{"text", "text_de"};
//    final DirectIndexDataProvider dataProv = new DirectIndexDataProvider();
//    new Environment.Builder(indexDir, dataDir)
//            .fields(fields)
//            .dataProvider(dataProv)
//            .loadOrCreateCache("testRun")
//            .autoWarmUp()
//            .build();
//    LOG.info("\n--- Default Clarity Score");
//    final DefaultClarityScore dcs = new DefaultClarityScore();
//    dcs.loadOrCreateCache("testRun");
//    dcs.preCalcDocumentModels(); // pre-calculate, if needed
//    dcs.calculateClarity(queryString);
//    LOG.info("\n--- Simplified Clarity Score");
//    Scoring.newInstance(Scoring.ClarityScore.SIMPLIFIED).
//            calculateClarity(queryString);
//    LOG.info("\n--- Improved Clarity Score");
//    final ImprovedClarityScore ics = new ImprovedClarityScore();
//    ics.loadOrCreateCache("testRun");
//    ics.preCalcDocumentModels();
//    ics.calculateClarity(queryString);
    LOG.info("Closing data provider & lucene index.");
    Environment.shutdown();
  }

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  /**
   * @param args Command line parameters
   * @throws IOException If index could not be read
   * @throws org.apache.lucene.queryparser.classic.ParseException If the {@link
   * Query} could not be parsed
   * @throws DocumentModelException If a requested document model needed for
   * calculation could not be created
   */
  public static void main(final String[] args)
      throws Exception {
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
    LOG.debug("FIN");
    System.exit(0);
  }
}
