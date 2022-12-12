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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class ClickstreamLister {

    /**
     * The directory where to load the data from.
     */
    private final String directory;

    /**
     * The feedname, per default the report suite id.
     */
    private final String feedname;

    /**
     * The name of the lookup that should be read.
     */
    private final String lookupname;

    /**
     * Flag denoting whether to MD5 calculation of files and its comparison with the MD5 hashes contained in the
     * Manifest.
     */
    private boolean validate = false;

    /**
     * TODO
     */
    private boolean joinLookups = false;

    /**
     * The date matching the manifest in case a single date delivery should be read.
     */
    private LocalDate date;

    /**
     * The dates for which the data should be read.
     */
    private List<LocalDate> dates;

    /**
     * Lower bound if a date range should be read.
     */
    private LocalDate from;

    /**
     * Upper bound if a date range should be read.
     */
    private LocalDate to;

    /**
     * The list of Manifests found according to the options.
     */
    private final List<Manifest> manifests;

    /**
     * The schema derived from the column_headers.tsv.
     */
    private StructType schema;

    /**
     * @param directory The directory.
     * @param feedname  The feedname.
     * @param options   Map containing the options passed.
     */
    public ClickstreamLister(final String directory, final String feedname, final String lookupname, final Map<String, String> options) {
        this.directory = directory;
        this.feedname = feedname;
        this.lookupname = lookupname;
        // Configure lister according to optional options
        configure(options);
        // Try to read the manifest according to the provided options
        try {
            this.manifests = getManifests();
        } catch (IOException e) {
            throw new IllegalStateException("Getting Manifests failed due to " + e.getMessage());
        }
    }

    /**
     * Configures this ClickstreamLister according to the options.
     *
     * @param options A Map containing the options.
     */
    private void configure(final Map<String, String> options) {
        // Check which options are present and parse them
        for (Map.Entry<String, String> e : options.entrySet()) {
            switch (e.getKey()) {
                case Options.VALIDATE:
                    this.validate = Boolean.parseBoolean(options.get(Options.VALIDATE));
                case Options.DATE:
                    this.date = LocalDate.parse(options.get(Options.DATE));
                    break;
                case Options.DATES:
                    String[] dates = options.get(Options.DATES).split(",");
                    this.dates = new ArrayList<>();
                    for (String date : dates) {
                        this.dates.add(LocalDate.parse(date));
                    }
                    break;
                case Options.FROM:
                    from = LocalDate.parse(options.get(Options.FROM));
                    break;
                case Options.TO:
                    to = LocalDate.parse(options.get(Options.TO));
                    break;
                default:
                    break;
            }
        }
        // Check if the set of arguments is valid. Invalid are for example a daterange as well as a list of dates in
        // combination.

        // Check for mutual exclusivity, conditions are verbose and can be "simplified" at the expense of readability
        if (date != null) {
            // check if all the other are null
            if (from != null || to != null || dates != null) {
                throw new IllegalStateException("Options contain more than one type of date definition!");
            }
        }

        if (dates != null) {
            // check if all the other are null
            if (from != null || to != null || date != null) {
                throw new IllegalStateException("Options contain more than one type of date definition!");
            }
        }

        if (from != null) {
            // check if to is set and all the other are null
            if (to == null || date != null || dates != null) {
                throw new IllegalStateException("Options contain more than one type of date definition!");
            }
        }

        if (to != null) {
            // check if from is set and all the other are null
            if (from == null || date != null || dates != null) {
                throw new IllegalStateException("Options contain more than one type of date definition!");
            }
        }

        // Check validity of from and to if provided
        if (from != null && to != null) {
            if (from.isAfter(to)) {
                throw new IllegalStateException("From date is after to date!");
            }
        }

        if (dates != null) {
            // Check if all dates are unique
            if (this.dates.size() < 1) {
                throw new IllegalStateException("Date list must have at least one entry");
            }

            // check for uniqueness of provided dates
            Set<LocalDate> dateSet = new HashSet<>(this.dates);
            if (dateSet.size() != this.dates.size()) {
                throw new IllegalStateException("Date list contains non unique entries!");
            }
        }
    }

    /**
     * Gets a list of Manifest objects denoted by the options.
     *
     * @return list of Manifest objects
     * @throws IOException Thrown if a manifest can't be found.
     */
    public List<Manifest> getManifests() throws IOException {
        List<Manifest> manifests = new ArrayList<>();
        if (date != null) {
            manifests.add(getManifestForDate(date));
        } else if (from != null && to != null) {
            manifests.addAll(getManifestsForDateRange(from, to));
        } else {
            manifests.addAll(getAllAvailableManifests());
        }

        return manifests;
    }

    /**
     * @param date
     * @return
     * @throws IOException
     */
    private Manifest getManifestForDate(final LocalDate date) throws IOException {
        return new Manifest(this.directory, feedname + "_" + date.toString() + ".txt", this.validate);
    }

    /**
     * @param from
     * @param to
     * @return
     * @throws IOException
     */
    private List<Manifest> getManifestsForDateRange(final LocalDate from, final LocalDate to) throws IOException {
        List<Manifest> manifests = new ArrayList<>();
        LocalDate current = LocalDate.parse(from.toString());
        while (current.isBefore(to)) {
            manifests.add(getManifestForDate(current));
            current = current.plusDays(1);
        }
        return manifests;
    }

    /**
     * @return
     * @throws IOException
     */
    private List<Manifest> getAllAvailableManifests() throws IOException {
        List<Manifest> manifests = new ArrayList<>();

        Pattern manifestPattern = Pattern.compile(this.feedname + "_\\d{4}-\\d{2}-\\d{2}\\.txt");
        File dir = new File(directory);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalStateException("Given directory does not exists or is no directory!");
        }
        File[] files = dir.listFiles();
        if (files == null) {
            throw new IllegalStateException("Directory does not contain any file!");
        }
        for (File f : files) {
            Matcher m = manifestPattern.matcher(f.getName());
            if (m.matches()) {
                manifests.add(new Manifest(this.directory, f.getName(), this.validate));
            }
        }
        return manifests;
    }

    /**
     * Assembles the typed schema by picking the column_headers.tsv of an arbitrary Manifest Lookup file.
     * This method ensures that all available Data Feeds share the same columns. If this is not the case the method
     * will throw a runtine exception.
     * <p>
     * For the actual types the Utils.DATATYPEMAPPING is used. If theres no data type available a StringType is used.
     *
     * @return A StructType containing proper Spark DataTypes for all columns.
     */
    public StructType getSchema() {
        if (this.schema == null) {
            try {
                // Generate generic lookup header
                if (this.getLookupname() != null) {
                    StructField[] structFields = new StructField[]{
                            DataTypes.createStructField("lookup_key", DataTypes.StringType, true),
                            DataTypes.createStructField("lookup_value", DataTypes.StringType, true),
                    };
                    this.schema = DataTypes.createStructType(structFields);
                } else {
                    // Generate the actual feed schema ensuring that all deliveries for the individual dates have the
                    // same shape
                    List<String> tmp = null;
                    for (Manifest m : this.manifests) {
                        List<String> headers = m.getLookupFile().getHeader();
                        if (tmp == null) {
                            tmp = headers;
                            continue;
                        }
                        if (tmp.size() != headers.size()) {
                            throw new IllegalStateException("Number of header fields differ!");
                        }
                        if (!tmp.containsAll(headers)) {
                            throw new IllegalStateException("Different header fields!");
                        }
                    }

                    if (tmp == null) {
                        throw new IllegalStateException("No Headers found!");
                    }

                    // Assemble the actual StructType
                    StructField[] structFields = new StructField[tmp.size()];
                    for (int i = 0; i < tmp.size(); i++) {
                        if (Utils.DATATYPEMAPPING.containsKey(tmp.get(i))) {
                            structFields[i] = Utils.DATATYPEMAPPING.get(tmp.get(i));
                        } else {
                            // Fallback if a new or unmaintained column is available
                            structFields[i] = DataTypes.createStructField(tmp.get(i), DataTypes.StringType, true);
                        }
                    }
                    this.schema = DataTypes.createStructType(structFields);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Unable to get schema due to: " + e.getMessage());
            }
        }
        return this.schema;
    }

    /**
     * Assembles a String Array containing all Data File paths given the manifests in scope.
     *
     * @return A String Array of Paths.
     */
    public String[] getDataFilePathes() {
        List<String> dataFiles = new ArrayList<>();
        for (Manifest m : this.manifests) {
            dataFiles.addAll(Arrays.asList(m.getDataFilePaths()));
        }
        String[] result = new String[dataFiles.size()];
        return dataFiles.toArray(result);
    }

    /**
     * Returns whether to validate the data or not.
     *
     * @return validate
     */
    public boolean getValidate() {
        return this.validate;
    }

    /**
     * Gets the number of rows across all Manifests.
     *
     * @return The sum of totalNumRecords of all Manifests.
     */
    public long getNumRows() {
        long sumOfRows = 0;
        for (Manifest m : this.manifests) {
            sumOfRows += m.totalNumRecords;
        }
        return sumOfRows;
    }

    /**
     * Gets the most recent Manifest.
     *
     * @return The manifest with the most recent date.
     */
    public Manifest getMostRecentManifest() {
        this.manifests.sort(Comparator.comparing(Manifest::getDate));
        return this.manifests.get(this.manifests.size() - 1);
    }

    public String getLookupname() {
        return this.lookupname;
    }

}
