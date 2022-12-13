# Clickstream Data Source

Adobe's Analysis Workspaces are great to generate recurring reports and occasionally diving deeper into the collected
Web-Analytics data. However, there are situations where you'll hit the limits of it and working on raw data is
required (e.g. Customer Journey calculation, Machine Learning, etc.). 
Luckily Adobe provides the collected raw data in form of [Data Feeds](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-overview.html?lang=en).

These feeds can become quite big easily, which may result in additional challenges when trying to process them.

One possibility to tackle the amount of data is to utilize distributed processing such as [Apache Spark](https://spark.apache.org/).

Despite the plain amount the way how the raw data is delivered (multi file delivery, separate lookups) generates 
significant overhead as well. 

A typical single day delivery looks like:

    .
    ├── 01-zwitchdev_2015-07-13.tsv.gz              # Data File containing the raw data
    ├── zwitchdev_2015-07-13-lookup_data.tar.gz     # All lookups wrapped in a single compressed tar
    └── zwitchdev_2015-07-13.txt                    # The manifest carrying meta data

In case you don't have a Data Feed at hand, [Randy Zwitch](https://randyzwitch.com/adobe-analytics-clickstream-raw-data-feed/) was so nice providing an example Feed. 
You can buy him a coffee [here!](https://github.com/sponsors/randyzwitch).

An example Manifest downloaded from the link above looks like this:
```text
Datafeed-Manifest-Version: 1.0
Lookup-Files: 1
Data-Files: 1
Total-Records: 592

Lookup-File: zwitchdev_2015-07-13-lookup_data.tar.gz
MD5-Digest: 5fc56e6872b75870d9be809fdc97459f
File-Size: 3129118

Data-File: 01-zwitchdev_2015-07-13.tsv.gz
MD5-Digest: 994c3109e0ddbd7c66c360426eaf0cd1
File-Size: 75830
Record-Count: 592
```
Usually the Manifest is written at last by Adobe, therefore indicating that the delivery is completed.
However, the delivery itself does not ensure that all Data Files (the example contains only one, but there can be a lot 
of them depending on the traffic your site generates) are delivered without any error.

Having the Manifest allows us to extract the files that should be delivered and do some checks (does the number of
delivered files match the expected one, ensure contents integrity, ...).

Integrating this into an ETL workflow can slow down the development speed, especially when it's the first time working
with Data Feeds.

The goal of this project is to speed up the initial process by providing a thin wrapper around Sparks reading CSV
capabilities specifically tailored for Adobe Data Feeds, allowing you to read (and validate) Data Feeds in a single line 
of code.

**Note:** This repository only supports reading Data Feeds from the local filesystem. For other filesystem get in contact
with [us](mailto:dp.team@feld-m.de) or fork this repository. In case you want to use the library in one of your commercial 
projects and GPL v3 does not fit, drop us a note.

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

Applying only these options all available Data Feeds stored in the directory denoted by `dir` will be read.

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
