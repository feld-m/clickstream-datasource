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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Representation of a Lookup File entry of a Manifest file.
 */
public class LookupFile {

    /**
     * The directory the lookup file represented by this instance is located in.
     */
    private final String directory;

    /**
     * The name of the lookup archive as denoted by the Manifest.
     */
    private final String name;

    /**
     * MD5 hash of the archive.
     */
    private final String md5;

    /**
     * The size of the archive.
     */
    private final int size;

    /**
     * Constructor.
     *
     * @param directory The directory the lookup file represented by this instance is located in.
     * @param name      The name of lookup file as denoted by the Manifest.
     * @param md5       The MD5 hash of the archive.
     * @param size      The size of the archive.
     */
    public LookupFile(final String directory, final String name, final String md5, final int size) {
        this.directory = directory;
        this.name = name;
        this.md5 = md5;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public String getMd5() {
        return md5;
    }

    public int getSize() {
        return size;
    }

    /**
     * Returns the full path of the lookup file represented by this instance.
     *
     * @return A String denoting the path this file is stored.
     */
    public String getPath() {
        return this.directory + File.separator + this.name;
    }

    /**
     * Extract the archive, reads column_headers.tsv and returns it as a list.
     * Additionally header names containing specific chars will be replaced by "_".
     *
     * @return A List containing the headers.
     * @throws IOException Something went wrong.
     */
    public List<String> getHeader() throws IOException {
        String regex = "[ ,;\\{\\}\\(\\)\\n\\t=]";
        List<String> sanitizedHeaders = new ArrayList<>();
        String lookupData = getLookup("column_headers.tsv");
        for (String s : lookupData.split("\t")) {
            s = s.replaceAll(regex, "_");
            sanitizedHeaders.add(s);
        }
        return sanitizedHeaders;
    }


    protected String extractLookup(final String lookupName) throws IOException {
        String[] splitted = lookupName.split("\\.");
        File f = File.createTempFile(splitted[0], splitted[1]);
        f.deleteOnExit();

        String content = getLookup(lookupName);

        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(f));
            bw.write(content);
            bw.flush();
            bw.close();
        } catch (IOException e) {
            throw new IllegalStateException("Error while writing lookup file due to " + e.getMessage());
        }
        return f.getAbsolutePath();
    }

    /**
     * Method to extract the archive and return the contents of a specific file contained in the archive.
     *
     * @param lookupName The lookup of interest.
     * @return A String containing the contents of the lookup.
     * @throws IOException Something went wrong.
     */
    protected String getLookup(final String lookupName) throws IOException {
        TarArchiveInputStream tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(getPath())));
        TarArchiveEntry currentEntry = tarInput.getNextTarEntry();
        BufferedReader br;
        StringBuilder sb = new StringBuilder();
        while (currentEntry != null) {
            if (currentEntry.getName().equals(lookupName)) {
                br = new BufferedReader(new InputStreamReader(tarInput));
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line).append("\n");
                }
                br.close();
                break;
            }
            currentEntry = tarInput.getNextTarEntry();
        }
        tarInput.close();
        return sb.toString();
    }

}
