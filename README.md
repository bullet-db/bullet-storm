# Bullet

[![Build Status](https://travis-ci.org/yahoo/bullet-storm.svg?branch=master)](https://travis-ci.org/yahoo/bullet-storm) [![Coverage Status](https://coveralls.io/repos/github/yahoo/bullet-storm/badge.svg?branch=master)](https://coveralls.io/github/yahoo/bullet-storm?branch=master) [![Download](https://api.bintray.com/packages/yahoo/maven/bullet-storm/images/download.svg) ](https://bintray.com/yahoo/maven/bullet-storm/_latestVersion)

## Table of Contents
1. [Introduction](#introduction)
2. [Storm](#storm-drpc)
3. [Query](#query)
    1. [Types](#types)
    2. [Filters](#filters)
        1. [Logical Filters](#logical-filters)
        2. [Relational Filters](#relational-filters)
    3. [Projections](#projections)
    4. [Aggregations](#aggregations)
        1. [Coming Soon](#coming-soon)
    5. [Termination Conditions](#termination-conditions)
    6. [Results](#results)
4. [Examples](#examples)
    1. [Simplest Query](#simplest-query)
    2. [Simple Filtering](#simple-filtering)
    3. [Relational Filters and Projections](#relational-filters-and-projections)
    4. [Logical Filters and Projections](#logical-filters-and-projections)
    5. [GROUP ALL COUNT Aggregation](#group-all-count-aggregation)
    6. [GROUP ALL Multiple Aggregations](#group-all-multiple-aggregations)
    7. [Exact COUNT DISTINCT Aggregation](#exact-count-distinct-aggregation)
    8. [Approximate COUNT DISTINCT Aggregation](#approximate-count-distinct-aggregation)
    9. [GROUP by Aggregation](#group-by-aggregation)
5. [Configuration](#configuration)
6. [Installation](#installation)
    1. [Older Storm Versions](#older-storm-versions)
7. [Launch](#launch)

## Introduction
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
| GROUP          | The resulting output would be a record containing the result of an operation for each unique group in the specified fields |
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
| COUNT          | Computes the number of the elements in the group |
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

### Termination Conditions

A query terminates when the following conditions are reached (this is configurable when launching Bullet but this is the default behavior):

1. A particular duration is reached. Whatever has been collected thus far at that time will be returned. The default duration is 30000 ms. If a duration is not provided in the query, 30000 is used instead. The maximum duration is 120000 ms. Anything greater will be clamped to 120000 ms. Both these can be configured in the settings. The maximum time a query can run for depends on the maximum time Storm DRPC request can last in your Storm topology.

2. A particular number of records have been collected. If no size is provided, a default of 1 is used. The default maximum is 30. Records will be collected till 30 is reached, if a size > 30 is provided.

If negative values are given for size and duration, the defaults of 1 and 30000 are used respectively.

### Results

Bullet results are JSON objects with two fields:

| Field   | Contents |
| ------- | -------- |
| records | This field contains the list of matching records |
| meta    | This field is a map that contains meta information about the query, such as the time the query was received, error data, etc. These are configurable at launch time. See [Configuration](#configuration) |

Examples of Bullet responses can be found below.

## Examples

The following examples are all sourced from Bullet running on raw, user events generated by instrumentation on Yahoo sites (Note the actual data shown here has been edited and is not how actual Yahoo user events look).

### Simplest Query

The simplest query you can write would be:
```javascript
{}
```
While not a very useful query - this will get any one event record (no filters => any record would be matched,
no projection => gets the entire record, default aggregation => LIMIT 1, default duration => 30000), this can be
used to quickly test your connection to Bullet.

### Simple Filtering

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

### Relational Filters and Projections

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


### Logical Filters and Projections

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


### GROUP ALL COUNT Aggregation
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


### GROUP ALL Multiple Aggregations

COUNT is the only GROUP operation for which you can omit a "field".

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


### Exact COUNT DISTINCT Aggregation

```javascript
{
  "aggregation": {
      "type": "COUNT DISTINCT",
      "fields": {
          "browser_name": "",
          "browser_version": ""
      }
    }
}
```

This gets the count of the unique browser names and versions in the next 30 s (default duration). Note that we do not specify values for the keys in fields. This is because they are not relevant

```javascript
{
    "records": [
        {
            "COUNT DISTINCT": 158.0
        }
    ],
    "meta": {
        "rule_id": 4451146261377394443,
        "aggregation": {
            "standardDeviations": {
                "1": {
                    "upperBound": 158.0,
                    "lowerBound": 158.0
                },
                "2": {
                    "upperBound": 158.0,
                    "lowerBound": 158.0
                },
                "3": {
                    "upperBound": 158.0,
                    "lowerBound": 158.0
                }
            },
            "wasEstimated": false,
            "sketchFamily": "COMPACT",
            "sketchTheta": 1.0,
            "sketchSize": 1280
        },
        "rule_body": "<RULE BODY HERE>}",
        "rule_finish_time": 1484084869073,
        "rule_receive_time": 1484084832684
    }
}
```

There were 158 unique combinations on browser names and versions in our dataset for those 30 seconds. Note the new ```aggregation``` object in the meta. It has various metadata about the result and Sketches. In particular, the ```wasEstimated``` key denotes where the result
was estimated or not. The ```standardDeviations``` key denotes the confidence at various sigmas: 1 (1 sigma = ~68% confidence, 2 sigma = ~95% confidence, 3 sigma = ~99% confidence). Since this result was not estimated, the result is the same as the upper and lower bounds for the result.


### Approximate COUNT DISTINCT Aggregation

```javascript
{
  "aggregation": {
      "type": "COUNT DISTINCT",
      "fields": {
          "ip_address": ""
      },
      "attributes": {
          "newName": "uniqueIPs"
      }
    },
    "duration": 10000
}
```

This query gets us the unique IP addresses in the next 10 s. It renames the result column from "COUNT DISTINCT" to "uniqueIPs".

```javascript
{
    "records": [
        {
            "uniqueIPs": 130551.07952805843
        }
    ],
    "meta": {
        "rule_id": 5377782455857451480,
        "aggregation": {
            "standardDeviations": {
                "1": {
                    "upperBound": 131512.85413760383,
                    "lowerBound": 129596.30223107953
                },
                "2": {
                    "upperBound": 132477.15103015225,
                    "lowerBound": 128652.93906100772
                },
                "3": {
                    "upperBound": 133448.49248615955,
                    "lowerBound": 127716.46773622213
                }
            },
            "wasEstimated": true,
            "sketchFamily": "COMPACT",
            "sketchTheta": 0.12549877074343688,
            "sketchSize": 131096
        },
        "rule_body": "<RULE BODY HERE>}",
        "rule_finish_time": 1484090240812,
        "rule_receive_time": 1484090223351
    }
}
```

The number of unique IPs in our dataset was 130551 in those 10 s (approximately) with the true value between (129596, 131512) at 68% confidence, (128652, 132477) at 95% confidence and (127716, 133448) at 99% confidence. In the *worst* case at 3 sigma (99% confidence),
our error is 2.17%. The final result was computed with 131096 bytes or ~128 KiB as denoted by ```sketchSize```. This happens to be maximum size the the COUNT DISTINCT sketch will take up at the default nominal entries, so even if we had billions of unique IPs, the size will be the same and the error may be higher (depends on the distribution). For example, the error when the same query was run for 30 s was 2.28% at 99% confidence (actual unique IPs: 559428, upper bound: 572514). In fact, the worst the error can get at this
Sketch size is 2.34% as defined [here](https://datasketches.github.io/docs/Theta/ThetaErrorTable.html), *regardless of the number of unique entries added to the Sketch!*.

### GROUP by Aggregation

```javascript
{
   "filters":[
      {
         "field": "demographics",
         "operation": "!=",
         "values": ["null"]
      }
   ],
   "aggregation":{
      "type": "GROUP",
      "size": 50,
      "fields": {
          "demographics.country": "country",
          "device": ""
      },
      "attributes": {
         "operations": [
            {
               "type": "COUNT",
               "newName": "count"
            },
            {
               "type": "AVG",
               "field": "demographics.age",
               "newName": "averageAge"
            },
            {
               "type": "AVG",
               "field": "timespent",
               "newName": "averageTimespent"
            }
         ]
      }
   },
   "duration": 20000
}
```

This query groups by the country and the device and for each unique group gets the count, average age and time spent by the users for the next 20 seconds. It renames demographics.country to country and does not rename device. It limits the groups to 50. If there were more than
50 groups, the results would be a uniform sampling of the groups (but each group in the result would have the correct result). These parameters can all be tweaked [in the configuration](#configuration).

```javascript
{
  "records":[
    {
      "country":"uk",
      "device":"desktop",
      "count":203034,
      "averageAge":32.42523,
      "averageTimespent":1.342
    },
    {
      "country":"us",
      "device":"desktop",
      "count":1934030,
      "averageAge":29.42523,
      "averageTimespent":3.234520
    },
    <...and 41 other such records here>
  ],
  "meta":{
    "rule_id":1705911449584057747,
    "aggregation":{
      "standardDeviations":{
        "1":{
          "upperBound":43.0,
          "lowerBound":43.0
        },
        "2":{
          "upperBound":43.0,
          "lowerBound":43.0
        },
        "3":{
          "upperBound":43.0,
          "lowerBound":43.0
        }
      },
      "wasEstimated":false,
      "uniquesEstimate":43.0,
      "sketchTheta":1.0
    },
    "rule_body":"<RULE_BODY_HERE>",
    "rule_finish_time":1485217172780,
    "rule_receive_time":1485217148840
  }
}
```

We recieved 43 rows for this result. The maximum groups that was allowed for the instance of Bullet was 512. If there were more groups than the maximum specified by your configuration, **a uniform sample** across them would be chosen
for the result. However, for each group, the values computed (average, count) would be exact. The standard deviations, whether the result was estimated and the number of approximate uniques in the metadata would reflect the change.

If you asked for 50 rows in the aggregation (as the query did above) but there were more than 50 in the result (but < 512), the metadata would reflect the fact that the result was not estimated. You would still get a uniform sample
but by increasing your aggregation size higher, you could get the rest.

For readability, if you were just trying to get the unique values for a field or a set of fields, you could leave out the attributes section and specify your fields section. You could also call the type ```DISTINCT``` instead of
```GROUP``` to make that explicit. ```DISTINCT``` is just an alias for ```GROUP```.

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

### Older Storm Versions

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
