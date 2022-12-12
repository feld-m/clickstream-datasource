/*
 *   Copyright (C) 2022  FELD M GmbH
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.feldm.datasource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Representation of a Manifest file.
 */
public class Manifest {

    /**
     * The directory the Manifest os located in.
     */
    private final String directory;

    /**
     * The file name of the Manifest itself.
     */
    private final String manifestName;

    /**
     * Flag indicating whether the data that has been read should be validated or not.
     * Validation incorporates MD5 hashing of data and lookup files as well as a checking whether all
     * rows could be read.
     */
    private final boolean validate;

    /**
     * The version of the Manifest.
     */
    private String manifestVersion;

    /**
     * Number of lookup files as denoted by the Manifest
     */
    int numLookupFiles;

    /**
     * List storing LookupFile objects.
     */
    private final List<LookupFile> lookupFiles;

    /**
     * Number of data files as denoted by the Manifest.
     */
    int numDataFiles;

    /**
     * List storing DataFile objects.
     */
    private final List<DataFile> dataFiles;

    /**
     * Total number of records as denoted by the Manifest.
     */
    long totalNumRecords;

    /**
     * Constructor.
     *
     * @param directory    Directory the Manifest represented by this object is stored in.
     * @param manifestName Name of the Manifest.
     * @param validate     Boolean flag whether the data should be validated or not.
     * @throws IOException Something went wrong.
     */
    public Manifest(final String directory, final String manifestName, final boolean validate) throws IOException {
        this.directory = directory;
        this.manifestName = manifestName;
        this.validate = validate;
        this.lookupFiles = new ArrayList<>();
        this.dataFiles = new ArrayList<>();
        parse();
    }

    /**
     * Parses the Manifest denoted by the manifest name.
     * Extracts all meta data and creates tiny objects representing lookup and data files.
     * Additionally it checks whether the number of lookup/data files parsed matches the ones expected by the meta data
     * of the Manifest.
     *
     * @throws IOException Something went wrong.
     */
    private void parse() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(this.directory + File.separator +
                this.manifestName)));

        // Read entire content of Manifest file
        String line;
        StringBuilder sb = new StringBuilder();
        while ((line = br.readLine()) != null) {
            sb.append(line).append("\n");
        }
        br.close();
        String content = sb.toString();

        Matcher m = Utils.METADATAPATTERN.matcher(content);
        if (!m.find()) {
            throw new IllegalStateException("Unable to retrieve Meta Data from Manifest!");
        }

        this.manifestVersion = m.group(1);
        this.numLookupFiles = Integer.parseInt(m.group(2).trim());
        this.numDataFiles = Integer.parseInt(m.group(3).trim());
        this.totalNumRecords = Long.parseLong(m.group(4).trim());

        m = Utils.LOOKUPFILEPATTERN.matcher(content);

        while (m.find()) {
            LookupFile lf = new LookupFile(this.directory, m.group(1), m.group(2),
                    Integer.parseInt(m.group(3).trim()));

            if (validate) {
                final String md5 = Utils.md5(lf.getPath());
                if (!md5.equals(lf.getMd5())) {
                    throw new IllegalStateException("Validation failed expected hash " + lf.getMd5() + " got " + md5);
                }
            }

            this.lookupFiles.add(lf);
        }

        m = Utils.DATAFILEPATTERN.matcher(content);

        while (m.find()) {
            DataFile df = new DataFile(this.directory, m.group(1), m.group(2),
                    Integer.parseInt(m.group(3).trim()), Long.parseLong(m.group(4).trim()));

            if (validate) {
                final String md5 = Utils.md5(df.getPath());
                if (!md5.equals(df.getMd5())) {
                    throw new IllegalStateException("Validation failed expected hash " + df.getMd5() + " got " + md5);
                }
            }

            this.dataFiles.add(df);
        }

        if (lookupFiles.size() != numLookupFiles || dataFiles.size() != numDataFiles) {
            throw new IllegalStateException("Not all files could be extracted from Manifest file!");
        }
    }

    /**
     * Returns a String array containing all paths of the data files.
     *
     * @return An Array containing all paths of the data files as Strings.
     */
    public String[] getDataFilePaths() {
        String[] paths = new String[this.dataFiles.size()];
        for (int i = 0; i < this.dataFiles.size(); i++) {
            paths[i] = this.dataFiles.get(i).getPath();
        }
        return paths;
    }

    /**
     * Gets the lookup file. This code expects that theres only one LookupFile per Manifest.
     *
     * @return The LookupFile belonging to the Manifest represented by this instance.
     */
    public LookupFile getLookupFile() {
        if (this.lookupFiles.size() != 1) {
            throw new IllegalStateException("Got zero or more than one lookup file...");
        }
        return this.lookupFiles.get(0);
    }

    /**
     * Gets the date of the Manifest represented by this instance.
     *
     * @return a LocalData corresponding to the date denoted in manifests filename.
     */
    public LocalDate getDate() {
        Matcher m = Utils.DATEPATTERN.matcher(this.manifestName);
        return LocalDate.parse(m.group());
    }

}
