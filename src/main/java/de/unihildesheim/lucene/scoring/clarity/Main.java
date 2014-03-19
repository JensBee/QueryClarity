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
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocumentModelException;
import de.unihildesheim.lucene.index.CachedIndexDataProvider;
import de.unihildesheim.util.ConfigurationOLD;
import java.io.IOException;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
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
    ConfigurationOLD.initInstance("config");

    if (this.indexDir.isEmpty() || this.queryString.isEmpty()) {
      LOG.error("No index or query specified.");
      Runtime.getRuntime().exit(1);
    }

    // index field to operate on
    final String[] fields = new String[]{"text"};
    final String storageId = "clef";

    Environment env = new Environment(indexDir, storageId, fields);
    final CachedIndexDataProvider dataProv = new CachedIndexDataProvider(
            storageId);
    env.create(dataProv);
    if (!dataProv.tryGetStoredData()) {
      dataProv.recalculateData(false);
    }

    LOG.info("\n--- Default Clarity Score");
    Scoring.newInstance(Scoring.ClarityScore.DEFAULT).calculateClarity(
            queryString);
    LOG.info("\n--- Simplified Clarity Score");
    Scoring.newInstance(Scoring.ClarityScore.SIMPLIFIED).
            calculateClarity(queryString);

    LOG.trace("Closing lucene index.");
    dataProv.dispose();
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
