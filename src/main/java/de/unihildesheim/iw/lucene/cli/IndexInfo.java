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

import asg.cliche.CLIException;
import asg.cliche.Command;
import asg.cliche.Param;
import asg.cliche.Shell;
import asg.cliche.ShellFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple interface to get some basic Lucene index informations.
 *
 * @author Jens Bertram
 */
public final class IndexInfo {

  /**
   * CLI-parameter specifying the storage id to load.
   */
  @Parameter(names = CliParams.SHELL_EXEC_P, description
      = CliParams.SHELL_EXEC_U,
      required = false, variableArity = true)
  @SuppressWarnings({"CollectionWithoutInitialCapacity",
      "MismatchedQueryAndUpdateOfCollection"})
  private final List<String> runCommand = new ArrayList<>();
  /**
   * CLI-parameter to specify the Lucene index directory.
   */
  @Parameter(names = CliParams.INDEX_DIR_P, description
      = CliParams.INDEX_DIR_U,
      required = true)

  private String indexDir;
  /**
   * Reader to access Lucene index.
   */
  private IndexReader reader;

  /**
   * @param args the command line arguments
   * @throws java.io.IOException Thrown on low-level I/O errors
   */
  public static void main(final String[] args)
      throws IOException {
    final IndexInfo ii = new IndexInfo();
    final JCommander jc = new JCommander(ii);
    try {
      jc.parse(args);
    } catch (ParameterException ex) {
      System.out.println(ex.getMessage() + "\n");
      jc.usage();
      System.exit(1);
    }

    ii.start();
  }

  /**
   * List document fields from Lucene index.
   *
   * @throws java.io.IOException Thrown on low-level I/O errors
   */
  @Command(description = "List document fields from Lucene index.")
  public void listFields()
      throws IOException {
    final Fields idxFields = MultiFields.getFields(this.reader);
    for (final String field : idxFields) {
      System.out.println("field={" + field + "} docCount={" + this.reader.
          getDocCount(field) + "} sumDocFreq={" + this.reader.
          getSumDocFreq(field) + "} sumTotalTermFreq={" + this.reader.
          getSumTotalTermFreq(field) + "}");
    }
  }

  /**
   * Show general index statistics.
   */
  @Command(description = "General index statistics")
  public void stats() {
    System.out.println("MaxDoc: " + this.reader.maxDoc());
    System.out.println("Deletions: " + this.reader.hasDeletions() + " ("
        + this.reader.numDeletedDocs() + ")");
  }

  /**
   * Get summed term frequency for a field.
   *
   * @param fieldName Field name
   * @throws IOException Thrown on low-level I/O errors
   */
  @Command(description = "Get summed term frequency for a field.")
  public void getFieldOverallTermFrequency(
      @Param(name = "fieldName", description = "Fields name.")
      final String fieldName)
      throws IOException {
    final Fields idxFields = MultiFields.getFields(this.reader);
    final Terms fieldTerms = idxFields.terms(fieldName);

    System.out.println(fieldTerms.getSumTotalTermFreq());
  }

  /**
   * Get term frequencies for a field and each term.
   *
   * @param fieldName Field name
   * @throws IOException Thrown on low-level I/O errors
   */
  @Command(description
      = "Get term frequencies for a field.")
  public void getFieldTermFrequency(
      @Param(name = "fieldName", description = "Fields name.")
      final String fieldName)
      throws IOException {
    final Fields idxFields = MultiFields.getFields(this.reader);
    final Terms fieldTerms = idxFields.terms(fieldName);

    // ..check if we have terms..
    if (fieldTerms != null) {
      final TermsEnum fieldTermsEnum = fieldTerms.iterator(null);

      // ..iterate over them..
      BytesRef bytesRef = fieldTermsEnum.next();
      while (bytesRef != null) {
        // fast forward seek to term..
        if (fieldTermsEnum.seekExact(bytesRef)) {
          System.out.println(bytesRef.utf8ToString() + ", " + fieldTermsEnum.
              totalTermFreq());
        }
        bytesRef = fieldTermsEnum.next();
      }
    }
  }

  /**
   * Quit the instance.
   */
  @Command(abbrev = ShellCmds.QUIT_S, name = ShellCmds.QUIT_L, description
      = ShellCmds.QUIT_U)
  public void quit() {
    System.exit(0);
  }

  /**
   * Run the instance.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  private void start()
      throws IOException {
    try {
      // open index
      final Directory directory = FSDirectory.open(new File(this.indexDir));
      this.reader = DirectoryReader.open(directory);
    } catch (IOException ex) {
      ex.printStackTrace();
      System.exit(1);
    }

    final Shell shell = ShellFactory.createConsoleShell("cmd", "IndexInfo",
        this);
    if (!this.runCommand.isEmpty()) {
      final String[] command = this.runCommand.toArray(
          new String[this.runCommand.size()]);
      try {
        shell.processLine(StringUtils.join(command, " "));
      } catch (CLIException ex) {
        ex.printStackTrace();
      }
    } else {
      shell.commandLoop();
    }
  }
}
