# JavaMetaspaceAnalyzeApp

Spark App to find out which package uses most bytes in java metaspace

## Usage

Change path to the your `class_stats` file and run with.
 
    sbt run
    
Example output:

```
// Total bytes per package level. For example: "akka" = package level 0, "com.utils.Util" = package level 2.
+----------+------------------+
|sum(Total)|Package           |
+----------+------------------+
|134216    |Package level - 0 |
|13297040  |Package level - 1 |
|39105688  |Package level - 2 |
|212904    |Package level - 3 |
|482640    |Package level - 4 |
|33511464  |Package level - 5 |
|12320712  |Package level - 6 |
|3066728   |Package level - 7 |
|1211616   |Package level - 8 |
|1159312   |Package level - 9 |
|null      |Package level - 10|
|204815952 |All packages      |
+----------+------------------+

// Total bytes per classes in specific package
+-----------------------------------------------------------+----------+
|Package                                                    |sum(Total)|
+-----------------------------------------------------------+----------+
|controllers.companyapp.api                                 |12,390,504|
|controllers.platform.api                                   |6,753,936 |
|com.company.model.comp                                     |6,672,528 |
|com.company.model.something                                |6,538,416 |
|com.company.model.usefulab                                 |5,652,736 |
|com.company.model.product                                  |3,913,616 |
|controllers.ornotuseful                                    |3,203,480 |
|com.company.checkout.steps                                 |2,908,056 |
```
