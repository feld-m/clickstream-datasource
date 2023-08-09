[![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.feldm/clickstream-datasource/badge.svg)](https://maven-badges.herokuapp.com/maven-central/de.feldm/clickstream-datasource)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

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

Additionally, the Data-Files do not contain the header. This information is contained in a file `column_headers.tsv` within
the lookup .tar.gz.

Integrating the checks, extraction etc. into an ETL workflow can slow down the development speed, especially when it's the first time working
with Data Feeds.

The goal of this project is to speed up the initial process by providing a thin wrapper around Sparks reading CSV
capabilities specifically tailored for Adobe Data Feeds, allowing you to read (and validate) Data Feeds in a single line 
of code.

In case you need support or experiencing any issues, feel free to create an [issue](https://github.com/feld-m/clickstream-datasource/issues) 
or drop [us](mailto:dp.team@feld-m.de) a note.

# Quickstart

The Data Source seamlessly integrates into the Spark environment. Therefore, you can make use of it in any language
Spark has wrappers for.

The following gives an overview how to use the Data Source in Java, Python and R.

## Python
Add the package via the `--packages` option and provide the artifact coordinates
```shell
./pyspark --packages "de.feldm:clickstream-datasource:0.1.0"
```
Once the interpreter is ready all you have to do is:
```python
df = spark.read.format("clickstream") \
                .option("date", "2015-07-13") \
                .option("feedname", "zwitchdev") \
                .option("dir", "file:///my/dir/") \
                .load()

df.show(5)
```

## Java
If you want to integrate it into you project add the following dependency into your `pom.xml`
```xml
<dependency>
    <groupId>de.feldm</groupId>
    <artifactId>clickstream-datasource</artifactId>
    <version>0.1.0</version>
</dependency>
```

All you have to do is: 
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
                .option("dir", "file:///my/dir/")
                // loads the actual clickstream. To load a specific lookup pass the lookupname as argument
                // e.g. .load("events.tsv")
                .load();

        df.show(5);
    }
}
```

## R
Add the package via the `--packages` option and provide the artifact coordinates
```shell
./sparkR --packages "de.feldm:clickstream-datasource:0.1.0"
```
Once the interpreter is ready all you have to do is:

```R
df <- read.df(dir = 'file:///my/dir/', source = 'clickstream', date = '2015-07-13', feedname = 'zwitchdev')
head(df)
```

## Generic

Or add the library via `--packages` option when submitting jobs
```shell
./spark-submit --packages "de.feldm:clickstream-datasource:0.1.0" <file>
```

# Features
- All exported columns are automatically renamed
- Reading a single Data Feed e.g. 2023-01-01
- Reading a set of Data Feeds e.g 2023-01-01,2023-02-01,2023-03-01
- Reading a range of Data Feeds e.g. from 2023-01-01 to 2023-02-15
- Validation of delivery/parsing
  - optional checking of expected and actual MD5 sums
  - validation if delivery options (e.g. changes in the exported columns) are in line
- Loading individual lookups such as browser lookups

All of this can be done with a single line of code!

## Options

Mandatory options are:

- dir: The path where the Data Feed is located
  - the path needs to contain the scheme (e.g. file://, wasbs://, ... )
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

## Build from source

Setting up is straight forward:

1. Clone the repository
2. Run `mvn clean install` inside the cloned repository
3. Copy resulting jar located in `target/` to your `<SPARK_HOME>/jars/` directory
4. Read your Data Feeds