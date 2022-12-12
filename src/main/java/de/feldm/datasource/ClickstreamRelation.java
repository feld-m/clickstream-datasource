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

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;

public class ClickstreamRelation extends BaseRelation implements TableScan, Serializable {

    private SQLContext sqlContext;

    private ClickstreamLister lister;

    public void setClickstreamLister(ClickstreamLister lister) {
        this.lister = lister;
    }

    public ClickstreamLister getClickstreamLister() {
        return this.lister;
    }

    public void setSqlContext(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    @Override
    public StructType schema() {
        return this.getClickstreamLister().getSchema();
    }

    @Override
    public RDD<Row> buildScan() {
        String[] files = this.getClickstreamLister().getDataFilePathes();

        @SuppressWarnings("resource")
        JavaSparkContext sparkContext = new JavaSparkContext(sqlContext.sparkContext());

        final String lookupname = this.getClickstreamLister().getLookupname();

        if (lookupname != null && !Utils.LOOKUPNAMES.contains(lookupname)) {
            throw new IllegalStateException("Lookupname " + lookupname + " not in known lookups " + Utils.LOOKUPNAMES);
        }

        if (lookupname != null) {
            // Read the most recent lookup file as e.g. browsers are being updated constantly
            Manifest m = this.getClickstreamLister().getMostRecentManifest();
            LookupFile lf = m.getLookupFile();
            for (String lookupName : Utils.LOOKUPNAMES) {
                try {
                    final String lookupPath = lf.extractLookup(lookupName);
                    Dataset<Row> lookupDf = sqlContext.read().format("csv").schema(this.schema()).option("header", "false").option("delimiter", "\t").load(lookupPath);
                    return lookupDf.rdd();
                } catch (IOException e) {
                    throw new IllegalStateException("Unable to read lookup due to: " + e.getMessage());
                }
            }
        }

        Dataset<Row> df = sqlContext.read().format("csv").option("header", "false").option("delimiter", "\t").schema(this.schema()).load(files);

        // Validate that the number of rows read match the expected ones
        if (this.getClickstreamLister().getValidate()) {
            long actualRows = df.count();
            long expectedRows = this.getClickstreamLister().getNumRows();
            if (expectedRows != actualRows) {
                throw new IllegalStateException("Expected " + expectedRows + " rows but only got " + actualRows);
            }
        }

        return df.rdd();
    }
}
