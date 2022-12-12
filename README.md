# Clickstream Data Source

Adobe's Analysis Workspaces are great to generated recurring reports and occasionally diving deeper into the collected
Web-Analytics data. However, there are situations where you'll hit the limits of it and working on raw data is
required (e.g. Customer Journey calculation, Machine Learning, etc.). Luckily Adobe provides the collected raw data in
form
of [Data Feeds](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-overview.html?lang=en)
.

These feeds can become quite big easily, which may result in additional challenges when trying to process them.

One possibility to tackle the amount of data is to utilize distributed processing such
as [Apache Spark](https://spark.apache.org/).

Due to the way how the raw data is delivered (multi file delivery, separate lookups) getting to the point having
everything set up and finally work with the data, can take some significant amount of time.

The goal of this project is to speed up the initial process by providing a wrapper around Sparks reading CSV
capabilities specificall tailored for Adobe Data Feeds.

In case you don't have a Data Feed at
hand, [Randy Zwitch](https://randyzwitch.com/adobe-analytics-clickstream-raw-data-feed/) was so nice providing an
example Feed. You can buy him a coffee [here](https://github.com/sponsors/randyzwitch).

**Note:** This repository only supports reading Data Feeds from the local filesystem. For other filesystem get in contact
with us or fork this repository.

## Features

- Reading a single Data Feed
- Reading a set of Data Feeds
- Reading a range of Data Feeds
- Validation of delivery/parsing
- Loading individual lookups

All of this can be done with a single line of code!

## Setup

Setting up is straight forward:

1. Clone the repository
2. Run `mvn clean install` inside the cloned repository
3. Copy resulting jar located in `target/` to your `<SPARK_HOME>/jars/` directory
4. Read your Data Feeds

## Usage

The Data Source seamlessly integrates into the Spark environment. Therefore, you can make use of it in any language
Spark has wrappers for. The following gives an overview how to use the Data Source in Java, Python and R. Only
prerequisite is that the resulting jar has been copied to `<SPARK_HOME>/jars/`

### Java

```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test {
    public static void main(String[] args) {
        // Obtain a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Clickstream")
                .master("local[*]").getOrCreate();

        // Read a single Data Feed
        Dataset<Row> df = spark.read()
                .format("clickstream") // Tell spark to use the custom data source
                .option("date", "2015-07-13")
                .option("feedname", "zwitchdev")
                .options("dir", "/my/dir/")
                // loads the actual clickstream. To load a specific lookup pass the lookupname as argument
                // e.g. .load("events.tsv")
                .load();

        df.show(5);
    }
}
```

### Python

```python3
df = spark.read.format("clickstream") \
                .option("date", "2015-07-13") \
                .option("feedname", "zwitchdev") \
                .option("dir", "/my/dir/") \
                .load()

df.show(5)
```

### R

```R
df <- read.df(dir = '/my/dir/', source = 'clickstream', date = '2015-07-13', feedname = 'zwitchdev')
head(df)
```

## Options

Mandatory options are:

- dir: The path where the Data Feed is located
- feedname: The name of the Data Feed (usually the name of the Report Suite)

Applying only these options all available Data Feeds stored in the directory denoted by `path` will be read.

In case you want to load a specific lookup simply pass the name of the lookup as argument to the `load` function such
as `.load("event.tsv")`. It will return a Dataframe having two columns namely "lookup_key" and "lookup_value".

### Reading a single Data Feed

To read a single Data Feed

- date: The date of the Data Feed that should be read (e.g "2020-01-13")

### Reading a set of Data Feeds

To read a set of Data Feeds

- dates: The comma separated dates of the Data Feeds that should be read (e.g "2020-01-13,2020-01-19")

### Reading a range of Data Feeds

To read all Data Feeds for a given Date Range

- from: The lower bound of the Data Feeds that should be read (e.g "2020-01-01")
- to: The upper bound of the Data Feeds that should be read (e.g "2020-01-31")

Both dates are inclusive and gaps will throw an Error.

### Apply validation

To apply validation add the following option

- validate: "true" (default is "false")

The validation will check whether the actual MD5 sums of the files match the ones stated in the Manifest and ensures
that the number of records read from the individual files match the to the sum of the total records of the Manifest in
scope.  
Note: Calculating the MD5 sum is quite expensive.  
