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

package de.unihildesheim.iw.fiz.cli;

import de.unihildesheim.iw.cli.CliBase;
import de.unihildesheim.iw.storage.sql.TableField;
import de.unihildesheim.iw.storage.sql.TableFieldContent;
import de.unihildesheim.iw.storage.sql.topics.PassagesTable;
import de.unihildesheim.iw.storage.sql.topics.PassagesTable.Fields;
import de.unihildesheim.iw.storage.sql.topics.TopicsDB;
import de.unihildesheim.iw.storage.xml.topics.Meta.Data;
import de.unihildesheim.iw.storage.xml.topics.PassagesList;
import de.unihildesheim.iw.storage.xml.topics.TopicsXML;
import de.unihildesheim.iw.storage.xml.topics.TopicsXML.MetaTags;
import org.jetbrains.annotations.Nullable;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class ScoringDB
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ScoringDB.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();

  /**
   * Additional fields for {@link PassagesTable}.
   */
  enum EntryFields {
    /**
     * Sample number.
     */
    SAMPLE,
    /**
     * Scoring content type (sentence or term).
     */
    TYPE,
    /**
     * List of index fields used for queries.
     */
    IDXFIELDS;

    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  }

  /**
   * Database for storing scored topics.
   */
  private TopicsDB db;

  /**
   * Initialize the cli base class.
   */
  protected ScoringDB() {
    super("Scoring results database manager.",
        "Collect and manage scoring results in a central database.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   * @throws ClassNotFoundException Thrown if JDBC driver could not be loaded
   * @throws SQLException Thrown on low-level database errors
   * @throws IOException Thrown on low-level i/o errors
   */
  public static void main(final String... args)
      throws SQLException, ClassNotFoundException, IOException {
    new ScoringDB().runMain(args);
    Runtime.getRuntime().exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   * @throws ClassNotFoundException Thrown if JDBC driver could not be loaded
   * @throws SQLException Thrown on low-level database errors
   * @throws IOException Thrown on low-level i/o errors
   */
  private void runMain(final String... args)
      throws SQLException, ClassNotFoundException, IOException {
    new CmdLineParser(this.cliParams);
    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    // table manager instance
    this.db = new TopicsDB(this.cliParams.dbFile);

    // extend fields for scoring results
    final Collection<TableField> passagesTableFields =
        Arrays.stream(Fields.values())
            .map(Fields::getAsTableField)
            .collect(Collectors.toList());
    passagesTableFields.add(
        new TableField(EntryFields.SAMPLE.toString(),
            EntryFields.SAMPLE +
                " int"));
    passagesTableFields.add(
        new TableField(EntryFields.TYPE.toString(), EntryFields.TYPE +
            " char(8)"));
    passagesTableFields.add(
        new TableField(EntryFields.IDXFIELDS.toString(), EntryFields.IDXFIELDS +
            " char(200) not null"));

    // create tables
    final PassagesTable dataTable =
        new PassagesTable(passagesTableFields);
    dataTable.addDefaultFieldsToUnique();
    dataTable.addFieldToUnique(EntryFields.SAMPLE);
    dataTable.addFieldToUnique(EntryFields.TYPE);
    this.db.createTables(dataTable);

    @SuppressWarnings("UnnecessarilyQualifiedInnerClassAccess")
    final PassagesTable.Writer dataWriter =
        new PassagesTable.Writer(this.db.getConnection());

    if (this.cliParams.xmlFile != null) {
      LOG.info("Reading file: {}", this.cliParams.xmlFile);
    } else {
      LOG.info("Reading files from: {}", this.cliParams.collectPath);
      if (this.cliParams.collectPattern != null) {
        final Pattern pat = Pattern.compile(this.cliParams.collectPattern);
        Files.walk(this.cliParams.collectPath)
            .filter(Files::isRegularFile)
            .filter(p -> pat.matcher(p.getFileName().toString()).matches())
            .forEach(p -> {
              if (this.cliParams.listFilesOnly) {
                LOG.info("{}", p);
              } else {
                try {
                  LOG.info("Processing file: {}", p);
                  final TopicsXML xml = new TopicsXML(p.toFile());
                  final PassagesList pList = xml.getPassagesList();
                  if (pList == null) {
                    return;
                  }

                  final List<Data> meta = xml.getMeta().getData();
                  // query type
                  final Optional<Data> metaTypeData = meta.stream()
                      .filter(d -> d.getName().equals(
                          MetaTags.QUERY_TYPE.toString()))
                      .findFirst();
                  final String metaType = metaTypeData.isPresent() ?
                      metaTypeData.get().getValue() : null;
                  // sample
                  final Optional<Data> metaSampleData = meta.stream()
                      .filter(d -> d.getName().equals(
                          MetaTags.SAMPLE.toString()))
                      .findFirst();
                  final String sampleId = metaSampleData.isPresent() ?
                      metaSampleData.get().getValue() : null;
                  // index fields
                  final Optional<Data> metaFieldsData = meta.stream()
                      .filter(d -> d.getName().equals(
                          MetaTags.IDX_FIELDS.toString()))
                      .findFirst();
                  final String idxFields = metaFieldsData.isPresent() ?
                      metaFieldsData.get().getValue() : null;

                  pList.getPassages().stream()
                      .forEach(pgs -> {
                        final String source = pgs.getSource();

                        pgs.getP().stream().forEach(ple -> {
                          final String content = ple.getContent();
                          ple.getScore().stream().forEach(s -> {
                            try {
                              final TableFieldContent tfc =
                                  new TableFieldContent(dataTable);
                              tfc.setValue(Fields.IMPL, s.getImpl())
                                  .setValue(Fields.ISEMPTY, s.isEmpty())
                                  .setValue(Fields.LANG, ple.getLang())
                                  .setValue(Fields.SCORE, s.getScore())
                                  .setValue(Fields.CONTENT, content)
                                  .setValue(Fields.SOURCE, source)
                                  .setValue(EntryFields.IDXFIELDS, idxFields);
                              if (this.cliParams.queryType != null) {
                                tfc.setValue(EntryFields.TYPE,
                                    this.cliParams.queryType);
                              } else if (metaType != null) {
                                tfc.setValue(EntryFields.TYPE, metaType);
                              }
                              if (this.cliParams.sampleNo != null) {
                                tfc.setValue(EntryFields.SAMPLE,
                                    this.cliParams.sampleNo);
                              } else if (sampleId != null) {
                                tfc.setValue(EntryFields.SAMPLE, sampleId);
                              }
                              dataWriter.addContent(tfc);
                            } catch (final SQLException e) {
                              throw new RuntimeException(e);
                            }
                          });
                        });
                      });
                } catch (final JAXBException e) {
                  throw new RuntimeException(e);
                }
              }
            });
      }
    }
    dataWriter.close();
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(Params.class);

    /**
     * Target file file for writing scored claims.
     */
    @Option(name = "-dbfile", metaVar = "FILE", required = true,
        usage = "SQLite database file. Will be created, if not found.")
    File dbFile;

    @Nullable
    @Option(name = "-collect", metaVar = "PATTERN", required = false,
        usage = "Recursively collect all files matching the given pattern.")
    String collectPattern;

    @Nullable
    @Option(name = "-collectfrom", metaVar = "PATH", required = false,
        usage = "Directory to start collecting files. " +
            "Defaults to current directory.")
    Path collectPath = new File(System.getProperty("user.dir")).toPath();

    @Nullable
    @Option(name = "-file", metaVar = "FILE", required = false,
        usage = "Import a single file.")
    String xmlFile;

    @Nullable
    @Option(name = "-sample", metaVar = "NUMBER", required = false,
        usage = "Set the sample number for all imported files.")
    Integer sampleNo;

    @Option(name = "-list", required = false,
        usage = "List files that will be processed and exit.")
    boolean listFilesOnly;

    @Nullable
    @Option(name = "-type", metaVar = "(term|sentence)", required = false,
        usage = "Set the query type used.")
    String queryType;

    Params() {
    }

    /**
     * Check, if the defined files and directories are available.
     *
     * @throws IOException Thrown on low-level i/o-errors
     */
    void check() {
      if (this.xmlFile == null && this.collectPattern == null) {
        LOG.error("No file and no collect pattern specified. Nothing to do.");
        Runtime.getRuntime().exit(-1);
      }
    }
  }
}
