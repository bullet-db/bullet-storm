# Bullet

[![Build Status](https://travis-ci.org/yahoo/bullet-storm.svg?branch=master)](https://travis-ci.org/yahoo/bullet-storm) [![Coverage Status](https://coveralls.io/repos/github/yahoo/bullet-storm/badge.svg?branch=master)](https://coveralls.io/github/yahoo/bullet-storm?branch=master) [![Download](https://api.bintray.com/packages/yahoo/maven/bullet-storm/images/download.svg) ](https://bintray.com/yahoo/maven/bullet-storm/_latestVersion)

Bullet is a real-time query engine that lets you perform queries on streaming data **without a need for a persistence store**. This makes it
extremely **light-weight, cheap and fast**.

Bullet is a **look-forward query system**. Queries are submitted first and they operate on data that arrive after the query is submitted.

It is **multi-tenant** and can scale independently for more queries and for more data (in the first order).

Data can be fetched using a simple custom query language, which is structured JSON. We plan to support more interfaces later.

It is **pluggable**. Pretty much any data source that can be read from Storm. You simply convert your data into the BulletRecord data container format either
in a Spout or a Bolt and wire up Bullet to it, letting you query that data.

The Bullet project also provides a **UI and WebService** that are also pluggable for a full end-to-end solution for your querying needs.

This project implements Bullet on Storm using Storm DRPC.

There are many ways one can use Bullet and how it is used is largely determined by the data source it consumes. For instance, one of the ways Bullet is used in
production internally at Yahoo is having it sit on raw user events from Yahoo sites and apps. This lets developers internally validate **end-to-end** their
instrumentation code in their Continuous Delivery pipelines automatically. This is critical since the instrumentation powers all data-driven decisions at Yahoo including
machine learning, corporate KPI, analytics, personalization, targeting etc.

## Storm DRPC

This project is Bullet on [Storm](https://storm.apache.org/) and is built using [Storm DRPC](http://storm.apache.org/releases/1.0.0/Distributed-RPC.html). The
query is sent through a DRPC request to a running topology that filters and joins all records emitted from your (configurable) data source - either a Spout
or a topology component according to the query specification. The resulting matched records can be aggregated and sent back to the client.

## Query

Bullet queries allow you to filter and aggregate records. For convenience and reducing data being sent over the wire,
projections are also supported so that the records retrieved only have the fields desired. Queries and results are both
JSON objects.

Queries also support specifying how many records to collect and how long to collect them. These have maximums that are configurable (controllable when
you launch the instance of Bullet). If you are looking at raw events or doing an aggregation on a high cardinality dimension, your result can
have at most this maximum number of records. Similarly, each query also has a maximum duration (window size) that your duration will be clamped to if it
exceeds it.

### Types

Data read by Bullet is typed. We support these types currently:

#### Primitives

1. Boolean
2. Long
3. Double
4. String

#### Complex

1. Map of Strings to any of the Primitives
2. Map of Strings to any Map in 1.
3. List of any Map in 1.

Fields inside maps can be accessed using the '.' notation in queries. For example, myMap.key will access the key field inside the
myMap map. There is no support for accessing fields inside Lists as of yet. Only the entire list can be pulled for now.

### Filters

Bullet supports two kinds of filters:

1. Logical filters
2. Relational filters

#### Logical Filters

Logical filters allow you to combine other filter clauses with logical operations like AND, OR and NOT.

The current logical operators allowed in filters are:

| Logical Operator | Meaning |
| ---------------- | ------- |
| AND              | All filters must be true. The first false filter evaluated left to right will short-circuit the computation. |
| OR               | Any filter must be true. The first true filter evaluated left to right will short-circuit the computation. |
| NOT              | Negates the value of the first filter clause. The filter is satisfied iff the value is true. |

The format for a Logical filter is:

```javascript
{
   "operation": "AND | OR | NOT"
   "clauses": [
      {"operation": "...", clauses: [{}, ...]},
      {"field": "...", "operation": "", values: ["..."]},
      {"operation": "...", clauses: [{}, ...]}
      ...
   ]
}
```

Any other type of filter may be provided as a clause in clauses.

#### Relational Filters

Relational filters allow you to specify conditions on a field, using a comparison operator and a list of values.

The current comparisons allowed in filters are:

| Comparison | Meaning |
| ---------- | ------- |
| ==         | Equal to any value in values |
| !=         | Not equal to any value in values |
| <=         | Less than or equal to any value in values |
| >=         | Greater than or equal to any value in values |
| <          | Less than any value in values |
| >          | Greater than any value in values |
| RLIKE      | Matches using [Java Regex notation](http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html), any Regex value in values |

These operators are all typed based on the type of the left hand side from the Bullet record. If the elements on the right hand side cannot be
casted to the types on the LHS, those items will be ignored for the comparison.

The format for a Relational filter is:

```javascript
{
    "operation": "== | != | <= | >= | < | > | RLIKE"
    "field": "record_field_name | map_field.subfield",
    "values": [
        "string values",
        "that go here",
        "will be casted",
        "to the",
        "type of field"
    ]
}
```

**Multiple top level filters behave as if they are ANDed together. This exists as a means to preserve backward-compatibility.**

### Projections
Projections allow you to pull out only the fields needed and rename them (renaming is being supported in order to give
better names to fields pulled out from maps). If projections are not specified, the entire record is returned. If you are querying
for raw records, use projections to help reduce the load on the system and network.

```javascript
{
    "projection": {
        "fieldA": "newNameA",
        "fieldB": "newNameB"
    }
}
```

### Aggregations

Aggregations allow you to perform some operation on the collected records. They take an optional size to restrict
the size of the aggregation (this applies for aggregations high cardinality aggregations and raw records).

The current aggregation types that are supported are:

| Aggregation    | Meaning |
| -------------- | ------- |
| GROUP          | The resulting output would be a record containing the result of an operation for each unique group in the specified fields (only supported with no fields at this time, which groups all records) |
| COUNT DISTINCT | Computes the number of distinct elements in the fields. (May be approximate) |
| LIMIT          | The resulting output would be at most the number specified in size. |

We currently only support GROUP operations if there is no fields being grouped on. In other words, you get the results of the operation on all records that matched your filters.

The current format for an aggregation is (**note see above for what is supported at the moment**):

```javascript
{
    "type": "GROUP | COUNT DISTINCT | TOP | PERCENTILE | RAW",
    "size": <a limit on the number of resulting records>,
    "fields": {
        "fields": "newNameA",
        "that go here": "newNameB",
        "are what the": "newNameC",
        "aggregation type applies to": "newNameD"
    },
    "attributes": {
        "these": "change",
        "per": [
           "aggregation type"
        ]
    }
}
```

You can also use LIMIT as an alias for RAW. DISTINCT is also an alias for GROUP. These exist to make some queries read a bit better.

Currently we support GROUP aggregations on the following operations:

| Operation      | Meaning |
| -------------- | ------- |
| SUM            | Computes the sum of the elements in the group |
| MIN            | Returns the minimum of the elements in the group |
| MAX            | Returns the maximum of the elements in the group |
| AVG            | Computes the average of the elements in the group |

The following attributes are supported for GROUP:

Attributes for GROUP:
```javascript
    "attributes": {
        "operations": [
            {
                "type": "COUNT",
                "newName": "resultColumnName"
            },
            {
                "type": "SUM",
                "field": "fieldName",
                "newName": "resultColumnName"
            },
            {
                "type": "MIN",
                "field": "fieldName",
                "newName": "resultColumnName"
            },
            {
                "type": "MAX",
                "field": "fieldName",
                "newName": "resultColumnName"
            },
            {
                "type": "AVG",
                "field": "fieldName",
                "newName": "resultColumnName"
            }
        ]
    }
```

Attributes for COUNT DISTINCT:

```javascript
    "attributes": {
        "newName": "the name of the resulting count column"
    }
```

Note that the new names you specify in the fields map for aggregations do not apply. You must use the attributes here to give your resulting output count column a name.

See the [examples section](#examples) for a detailed description of how to perform these aggregations.

#### Coming Soon

It is often intractable to perform aggregations on an unbounded stream of data and support arbitrary queries. However, it is possible
if an exact answer is not required as long as the error is quantifiable. There are stochastic algorithms and data structures that let us
support these aggregations. We will be using [Data Sketches](https://datasketches.github.io/) to solve aggregations such as counting
uniques, getting distributions, approximating top k etc. Sketches let us be exact in our computation up to configured thresholds
and approximate after. The error is very controllable and mathematically provable. This lets us address otherwise hard to solve problems in
sublinear space. We will also use Sketches as a way to control high cardinality grouping (group by a natural key column or related) and rely on
the Sketching data structure to drop excess groups. It is up to the user launching Bullet to determine to set Sketch sizes large or
small enough for to satisfy the queries that will be performed on that instance of Bullet.

Using Sketches, we have implemented COUNT DISTINCT and are working on other aggregations including but not limited to:

| Aggregation    | Meaning |
| -------------- | ------- |
| GROUP          | We currently support GROUP with no fields (group all); grouping on specific fields will be supported soon |
| TOP K          | Returns the top K most freqently appearing values in the column |
| DISTRIBUTION   | Computes distributions of the elements in the column |

The following attributes are planned to be supported for the different aggregations:

Attributes for TOP K:

```javascript
    "attributes": {
        "k": 15,
    }
```

The attributes for the DISTRIBUTION aggregation haven't been decided yet.

### Constraints

In order to avoid DOS, we have the following constraints imposed on a query (this is configurable when launching Bullet but this is the default behavior):
1. A query can only last for a duration of 30000 ms. If one is not provided, 30000 is used instead. The maximum duration supported is 120000 ms. Anything greater will be clamped to 120000 ms.
2. The size in aggregation can at most be 30. Records will be collected till 30 is reached, if a size > 30 is provided. If no size is provided, a default of 1 is used.

If negative values are given for size and duration, the defaults of 1 and 30000 are used respectively.

### Return Value

Bullet responses are JSON objects with two fields:

| Field   | Contents |
| ------- | -------- |
| records | This field contains the list of matching records |
| meta    | This field is a map that contains meta information about the query, such as the time the query was received, error data, etc. These are configurable at launch time |

Examples of Bullet responses can be found below.

## Examples

The following examples are all sourced from Bullet running on raw, user events generated by instrumentation on Yahoo sites (Note the actual data shown here has been edited and is not how actual Yahoo user events look).

The simplest query you can write would be:
```javascript
{}
```
While not a very useful query - this will get any one event record (no filters => any record would be matched,
no projection => gets the entire record, default aggregation => LIMIT 1, default duration => 30000), this can be
used to quickly test your connection to Bullet.

A more useful example query could be:
```javascript
{
   "filters":[
       {
           "field":"id",
           "operation":"==",
           "values":[
               "btsg8l9b234ha"
           ]
       }
    ]
}
```

Because of the default constraints, this query would find at most 1 record with the id matching the value provided. The record would have all its fields.

A sample response could be (it has been edited to remove PII and other Yahoo data). The response contains a single matching record, and the associated meta information.

```javascript
{
   "records":[
       {
           "server_name":"EDITED",
           "page_uri":"/",
           "is_page_view":true,
           "device":"tablet",
           "debug_codes":{
               "http_status_code":"200"
           },
           "referrer_domain":"www.yahoo.com",
           "is_logged_in":true,
           "timestamp":1446842189000,
           "event_family":"view",
           "id":"btsg8l9b234ha",
           "os_name":"mac os",
           "demographics":{
               "age" : "25",
               "gender" : "m",
            }
       }
    ],
    "meta":{
        "rule_id":1167304238598842449,
        "rule_body":"{}",
        "rule_finish_time":1480723799550,
        "rule_receive_time":1480723799540
    }
}
```

A more complex example query specification could be:

```javascript
{
    "filters":[
        {
            "field":"id",
            "operation":"==",
            "values":[
                "btsg8l9b234ha"
            ]
        },
        {
            "field":"page_id",
            "operation":"!=",
            "values":[
                "null"
            ]
        }
    ],
    "projection":{
        "fields":{
            "timestamp":"ts",
            "device_timestamp":"device_ts",
            "event":"event",
            "page_domain":"domain",
            "id":"id"
        }
    },
    "aggregation":{
        "type":"RAW",
        "size":10
    },
    "duration":20000
}
```

The above query finds all events with id set to 'btsg8l9b234ha' and page_id is not null, projects out the fields
listed above with their new names (timestamp becomes ts etc) and limits the results to at most 10 such records.
RAW indicates that the complete raw record fields will be returned, and more complicated aggregations such as COUNT or
SUM will not be performed. The duration would set the query to wait at most 20 seconds for records to show up.

The resulting response could look like (only 3 events were generated that matched the criteria):

```javascript
{
    "records": [
        {
            "domain": "http://some.url.com",
            "device_ts": 1481152233788,
            "id": 2273844742998,
            "event": "page",
            "ts": null
        },
        {
            "domain": "www.yahoo.com",
            "device_ts": 1481152233788,
            "id": 227384472956,
            "event": "click",
            "ts": 1481152233888
        },
        {
            "domain": "https://news.yahoo.com",
            "device_ts": null,
            "id": 2273844742556,
            "event": "page",
            "ts": null
        }
    ],
    "meta": {
        "rule_id": -3239746252817510000,
        "rule_body": "<entire rule body is re-emitted here>",
        "rule_finish_time": 1481152233799,
        "rule_receive_time": 1481152233796
    }
}
```

Another mock example that uses logical filters is:

```javascript
{
 "filters": [
                {
                    "operation": "OR",
                    "clauses": [
                        {
                            "operation": "AND",
                            "clauses": [
                                {
                                    "field": "id",
                                    "operation": "==",
                                    "values": ["c14plm1begla7"]
                                },
                                {
                                    "operation": "OR",
                                    "clauses": [
                                        {
                                            "operation": "AND",
                                            "clauses": [
                                                {
                                                    "field": "experience",
                                                    "operation": "==",
                                                    "values": ["web"]
                                                },
                                                {
                                                    "field": "page_id",
                                                    "operation": "==",
                                                    "values": ["18025", "47729"]
                                                }
                                            ]
                                        },
                                        {
                                            "field": "link_id",
                                            "operation": "RLIKE",
                                            "values": ["2.*"]
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "operation": "AND",
                            "clauses": [
                                {
                                    "field": "tags.player",
                                    "operation": "==",
                                    "values": ["true"]
                                },
                                {
                                    "field": "demographics.age",
                                    "operation": ">",
                                    "values": ["65"]
                                }
                            ]
                        }
                    ]
                }
            ],
 "projection" : {
    "fields": {
        "id": "id",
        "experience": "experience",
        "page_id": "pid",
        "link_id": "lid",
        "tags": "tags",
        "demographics.age": "age"
    }
 },
 "aggregation": {"type" : "RAW", "size" : 1},
 "duration": 60000
}
```

This query can be rewritten in HiveQL (or SQL). The WHERE clause of the query would look like:

```
(id = "c14plm1begla7" AND ((experience = "web" AND page_id IN ["18025", "47729"]) OR link_id RLIKE "2.*"))
OR
(tags["player"] AND demographics["age"] > "65")
```

*Note: If demographics["age"] was of type Long, then Bullet will convert 85 to be an Long, but in this example, we are pretending that it is String.
So, no conversion is made. Similarly for link_id, id, experience and page_id. tags is a Map of String to Boolean so Bullet
converts "true" to the Boolean true.*

This query is looking for a single event with a specific id and either the page_id is in two specific pages on the "web"
experience or with a link_id that starts with 2, or a player event where the age is greater than "65".
In other words, it is looking for senior citizens who generate video player events or a particular person's
(based on id) events on two specific pages or a group of pages that have link that have ids that start with 2. It then projects out only these
fields with different names.

A sample result could look like (it matched because of tags.player was true and demographics.age was > 65):

```javascript
{
    "records": [
        {
            "pid":"158",
            "id":"0qcgofdbfqs9s",
            "experience":"web",
            "lid":"978500434",
            "age":"66",
            "tags":{"player":true}
        }
    ],
    "meta": {
        "rule_id": 3239746252812284004,
        "rule_body": "<entire rule body here>",
        "rule_finish_time": 1481152233805,
        "rule_receive_time": 1481152233881
    }
}
```

An example of a query performing a COUNT all records aggregation would look like:

```javascript
{
   "filters":[
      {
         "field": "demographics.age",
         "operation": ">",
         "values": ["65"]
      }
   ],
   "aggregation":{
      "type": "GROUP",
      "attributes": {
         "operations": [
            {
               "type": "COUNT",
               "newName": "numSeniors"
            }
         ]
      }
   },
   "duration": 20000
}
```

This query will count the number events for which demographics.age > 65. The aggregation type GROUP indicates that it is a group aggregation. To group by a key, the "fields"
key needs to be set in the "aggregation" part of the query. If "fields" is empty or is omitted (as it is in the query above) and the "type" is "GROUP", it is as if all the
records are collapsed into a single group - a GROUP ALL. Adding a "COUNT" in the "operations" part of the "attributes" indicates that the number of records in this
group will be counted, and the "newName" key denotes the name the resulting column "numSeniors" in the result. Setting the duration to 20000 counts matching records for
this duration.

A sample result would look like:

```javascript
{
    "records": [
        {
            "numSeniors": 363201
        }
    ],
    "meta": {}
}
```

This result indicates that 363,201 records were counted with demographics.age > 65 during the 20 seconds the query was running.

COUNT is the only GROUP operation for which you can omit a "field". A more complicated example with multiple group operations would look like:

```javascript
{
   "filters":[
      {
         "field": "demographics.state",
         "operation": "==",
         "values": ["california"]
      }
   ],
   "aggregation":{
      "type": "GROUP",
      "attributes": {
         "operations": [
            {
               "type": "COUNT",
               "newName": "numCalifornians"
            },
            {
               "type": "AVG",
               "field": "demographics.age",
               "newName": "avgAge"
            },
            {
               "type": "MIN",
               "field": "demographics.age",
               "newName": "minAge"
            },
            {
               "type": "MAX",
               "field": "demographics.age",
               "newName": "maxAge"
            }
         ]
      }
   },
   "duration": 20000
}
```

A sample result would look like:

```javascript
{
    "records": [
        {
            "maxAge": 94.0,
            "numCalifornians": 188451,
            "minAge": 6.0,
            "avgAge": 33.71828
        }
    ],
    "meta": {
        "rule_id": 8051040987827161000,
        "rule_body": "<RULE BODY HERE>}",
        "rule_finish_time": 1482371927435,
        "rule_receive_time": 1482371916625
    }
}
```

This result indicates that, among the records observed during the 20 seconds this query ran, there were 188,451 users with demographics.state == "california". Among these users the average age was 33.71828, the max
age observed was 94, and the minimum age observed was 6.

## Configuration

Bullet is configured at run-time using settings defined in a file. Settings not overridden will default to the values in [src/main/resources/bullet_defaults.yaml](src/main/resources/bullet_defaults.yaml).
You can find out what these settings do in the comments listed in the defaults.

## Installation

To use Bullet, you need to implement a way to read from your data source and convert your data into BulletRecords (bullet-record is a transitive dependency for Bullet and can be found
[in Bintray](https://bintray.com/yahoo/maven/bullet-record/view). You have two options in how to get your data into Bullet:

1. You can implement a Spout that reads from your data source and emits BulletRecord. This spout must have a constructor that takes a List of Strings.
2. You can pipe your existing Storm topology directly into Bullet. In other words, you convert the data you wish to be queryable through Bullet into BulletRecords from a bolt in your topology.

Option 2 *directly* couples your topology to Bullet and as such, you would need to watch out for things like backpressure etc.

You need a JVM based project that implements one of the two options above. You include the Bullet artifact and Storm dependencies in your pom.xml or other dependency management system. The artifacts
are available through JCenter, so you will need to add the repository.

```xml
    <repositories>
        <repository>
            <snapshots>
                <enabled>false</enabled>
            </snapshots>
            <id>central</id>
            <name>bintray</name>
            <url>http://jcenter.bintray.com</url>
        </repository>
    </repositories>
```

```xml
    <dependency>
      <groupId>org.apache.storm</groupId>
      <artifactId>storm-core</artifactId>
      <version>${storm.version}</version>
      <scope>provided</scope>
    </dependency>

    <dependency>
      <groupId>com.yahoo.bullet</groupId>
      <artifactId>bullet-storm</artifactId>
      <version>${bullet.version}</version>
    </dependency>
```

If you just need the jar artifact directly, you can download it from [JCenter](http://jcenter.bintray.com/com/yahoo/bullet/bullet-storm/).

If you are going to use the second option (directly pipe data into Bullet from your Storm topology), then you will need a main class that directly calls
the submit method with your wired up topology and the name of the component that is going to emit BulletRecords in that wired up topology. The submit method
can be found in [Topology.java](src/main/java/com/yahoo/bullet/Topology.java). The submit method submits the topology so it should be the last thing you do in your main.

If you are just implementing a Spout, see the [Launch](#launch) section below on how to use the main class in Bullet to create and submit your topology.

Storm topologies are generally launched with "fat" jars (jar-with-dependencies), excluding storm itself:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-assembly-plugin</artifactId>
    <version>2.4</version>
    <executions>
        <execution>
            <id>assemble-all</id>
            <phase>package</phase>
            <goals>
                <goal>single</goal>
            </goals>
        </execution>
    </executions>
    <configuration>
        <descriptorRefs>
            <descriptorRef>jar-with-dependencies</descriptorRef>
        </descriptorRefs>
    </configuration>
</plugin>
```

### Storm versions below 1.0

Since package prefixes changed from `backtype.storm` to `org.apache.storm` in Storm 1.0 and above, you will need to get the storm-0.10 version of Bullet if
your Storm cluster is still not at 1.0 or higher. You change your dependency to:

```xml
    <dependency>
      <groupId>com.yahoo.bullet</groupId>
      <artifactId>bullet-storm-0.10</artifactId>
      <version>${bullet.version}</version>
    </dependency>
```

The jar artifact can be downloaded directly from [JCenter](http://jcenter.bintray.com/com/yahoo/bullet/bullet-storm-0.10/).

Also, since storm-metrics and the Resource Aware Scheduler are not in Storm versions less than 1.0, there are changes in the Bullet settings. The settings
that set the CPU and memory loads do not exist (so remove them from the config file). The setting to enable topology metrics and the topology scheduler are
no longer present (you can still override these settings if you run a custom version of Storm by passing it to the storm jar command. [See below](#launch).)
You can take a look the settings file on the storm-0.10 branch in the Git repo.

If for some reason, you are running a version of Storm less than 1.0 that has the RAS backported to it and you wish to set the CPU and other settings, you will
your own main class that mirrors the master branch of the main class but with backtype.storm packages instead.

## Launch

If you have implemented your own main class (option 2 above), you just pass your main class to the storm executable as usual. If you are implementing a spout, here's an example of how you could launch the topology:

```bash
storm jar your-fat-jar-with-dependencies.jar \
          com.yahoo.bullet.Topology \
          --bullet-conf path/to/the/bullet_settings.yaml \
          --bullet-spout full.package.prefix.to.your.spout.implementation \
          --bullet-spout-parallelism 64 \
          --bullet-spout-cpu-load 200.0 \
          --bullet-spout-on-heap-memory-load 512.0 \
          --bullet-spout-off-heap-memory-load 256.0 \
          --bullet-spout-arg arg-to-your-spout-class-for-ex-a-path-to-a-config-file \
          --bullet-spout-arg another-arg-to-your-spout-class \
          -c topology.acker.executors=0
```

You can pass other arguments to Storm using the -c argument. The example above turns off acking for the Bullet topology. **This is recommended to do since Bullet does not anchor tuples and DRPC follows the convention
of leaving retries to the DRPC user (in our case, the Bullet web service).** If the DRPC tuples take longer than the default tuple acking timeout to be acked, your query will be failed even though it is still collecting
data. You could set the tuple acking timeout (topology.message.timeout.secs) to higher than the default of 30 and longer than your maximum query duration but since DRPC does not re-emit your query in case of a
failure, this is pointless anyway. The tuple tree will be kept around till the timeout needlessly. While you trade off query reliability and at least once processing guarantees, you can build retries into the query
submitter if this is important to you.

Code licensed under the Apache 2 license. See LICENSE file for terms.
