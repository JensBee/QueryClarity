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

import au.com.bytecode.opencsv.CSVReader;
import de.unihildesheim.iw.cli.CliBase;
import de.unihildesheim.iw.xml.elements.Passage;
import de.unihildesheim.iw.xml.elements.PassagesGroup;
import de.unihildesheim.iw.xml.elements.TopicPassages;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Convert term dumps listed as CSV file to scoring XML format.
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class TermDumpToScoringXml extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(TermDumpToScoringXml.class);

  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();

  /**
   * Private constructor initializing super class.
   */
  private TermDumpToScoringXml() {
    super("Convert term dumps listed as CSV file to scoring XML format.", "");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   * @throws IOException
   */
  public static void main(final String[] args)
      throws IOException, JAXBException {
    new TermDumpToScoringXml().runMain(args);
  }

  private void runMain(final String[] args)
      throws IOException, JAXBException {
    parseWithHelp(this.cliParams, args);

    this.cliParams.check();
    runExtraction();
  }

  private enum CSV_FLD {
    INDEX(0), TERM(1), RELDF(2), BIN(3), FIELD(4);


    final int idx;

    CSV_FLD(final int newIdx) {
      this.idx = newIdx;
    }
  }

  /**
   * Reads the CSV term list and converts them to XML.
   */
  private void runExtraction()
      throws IOException, JAXBException {

    final CSVReader csvReader = new CSVReader(new FileReader(this.cliParams
        .csvFile));

    final JAXBContext jaxbContext = JAXBContext.newInstance(
        TopicPassages.class,
        PassagesGroup.class,
        Passage.class);
    final Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
    // pretty-print output
    jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

    final TopicPassages topicPassages = new TopicPassages();

    // line[] = index, term, reldf, bin, field
    String[] line = csvReader.readNext();
    LOG.debug("header: {}", line);
    line = csvReader.readNext();
    while (line != null) {
      final StringBuilder srcStr = new StringBuilder();
      srcStr.append(line[CSV_FLD.FIELD.idx])
          .append(':')
          .append(line[CSV_FLD.BIN.idx])
          .append(':')
          .append(line[CSV_FLD.RELDF.idx]);

      final PassagesGroup pGroup = new PassagesGroup(srcStr.toString());
      pGroup.getPassages().add(
          new Passage(this.cliParams.lang, line[CSV_FLD.TERM.idx]));

      topicPassages.getPassageGroups().add(pGroup);
      line = csvReader.readNext();

      LOG.debug("read: {}", line);
    }
    jaxbMarshaller.marshal(topicPassages, this.cliParams.targetFile);
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Target file for extracted claims.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-out", aliases = "-o", usage = "Output file",
        metaVar = "FILE", required = true)
    File targetFile;
    /**
     * Topics file.
     */
    @Option(name = "-csvfile", metaVar = "FILE",
        required = true,
        usage = "CSV file containing the dumped term list")
    private File csvFile;
    /**
     * Language.
     */
    @Option(name = "-lang", metaVar = "language",
        required = true,
        usage = "CSV file terms language (2 char code)")
    private String lang;

    /**
     * Accessor for parent class.
     */
    Params() {
    }

    /**
     * Check parameters.
     *
     * @throws IOException Thrown, if the source directory could not be found
     */
    void check()
        throws IOException {
      if (!this.csvFile.exists()) {
        LOG.error("Topic file '" + this.csvFile + "' does not exist.");
        System.exit(1);
      }

      if (this.targetFile.exists()) {
        LOG.error("Target file '" + this.targetFile + "' exists.");
        System.exit(1);
      }
    }
  }
}
