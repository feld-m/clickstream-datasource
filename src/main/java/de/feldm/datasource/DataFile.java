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

import java.io.File;

/**
 * Representation of a Data File entry of a Manifest file.
 */
public class DataFile {

    /**
     * The directory the data file represented by this instance is located in.
     */
    private final String directory;

    /**
     * The name of the data file as denoted by the Manifest.
     */
    private final String name;

    /**
     * The MD5 hash of the file.
     */
    private final String md5;

    /**
     * The size of the file.
     */
    private final int size;

    /**
     * The number of records contained in this file.
     */
    private final long numRecords;

    /**
     * Constructor.
     *
     * @param directory  The directory the data file represented by this instance is located in.
     * @param name       The name of the data file as denoted by the Manifest.
     * @param md5        The MD5 hash of this file.
     * @param size       The size of this file.
     * @param numRecords The number of records contained in this file.
     */
    public DataFile(final String directory, final String name, final String md5, final int size, final long numRecords) {
        this.directory = directory;
        this.name = name;
        this.md5 = md5;
        this.size = size;
        this.numRecords = numRecords;
    }

    public final String getName() {
        return name;
    }

    public String getMd5() {
        return md5;
    }

    public int getSize() {
        return size;
    }

    public long getNumRecords() {
        return numRecords;
    }

    /**
     * Returns the full path of the lookup file represented by this instance.
     *
     * @return A String denoting the path this file is stored.
     */
    public String getPath() {
        return this.directory + File.separator + this.name;
    }
}
