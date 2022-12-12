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

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

public class ClickstreamDataSource implements RelationProvider, DataSourceRegister {

    @Override
    public BaseRelation createRelation(
            SQLContext sqlContext,
            Map<String, String> params) {
        // Convert Scala Map to Java Map
        java.util.Map<String, String> options = JavaConverters.mapAsJavaMapConverter(params).asJava();

        // Setting up the relation and pass sqlContext to it
        final ClickstreamRelation cr = new ClickstreamRelation();
        cr.setSqlContext(sqlContext);

        // Obtain mandatory options
        final String dir = options.get("dir");
        final String feedname = options.get("feedname");

        // parameter of the load() call - if any...
        // Only needs to be set if we want to read a certain lookup file.
        final String lookupname = options.get("path");

        // Create Lister and link it to relation
        final ClickstreamLister lister = new ClickstreamLister(dir, feedname, lookupname, options);
        cr.setClickstreamLister(lister);
        return cr;
    }

    @Override
    public String shortName() {
        return "clickstream";
    }
}