# :sparkles: Sparkles

A tool to analyze and tune Spark applications. It retrieves application data from `SparkListenerEvent` stream, checks them through a set of heuristic rules, and suggests possible optimizations based on some certain strategies.

## Build

Sparkles uses `sbt` as the build tool. Run `make help` to show avalable commands.

- To build fat jar:
  
  ```console
  sbt clean assembly
  ```

## Usage

The package exposes 2 main classes

### SparklesListener

Extending `org.apache.spark.scheduler.SparkListener`, this will analyze a Spark application as it's running.

Add the following arguments to `spark-submit` or `spark-shell`:

```console
--jars sparkles-assembly-{version}.jar \
--conf spark.extraListeners=data.seafin.sparkles.SparklesListener
```

### SparklesApp

Analyze Spark application using compressed event history file.

Run with

```console
spark-submit --class data.seafin.sparkles.SparklesApp sparkles-assembly-{version}.jar
```

### Configurations

Additional configurations to specify:

- `spark.sparkles.log.dir`: Hadoop/local directory to read event log files.
- `spark.sparkles.result.dir`: Hadoop/local directory to store result files.
- `spark.sparkles.appid`: Spark app id to read event log file.

## Reference

This app is influenced by other existing Spark profiling and tuning tools:

- [sparklens](https://github.com/qubole/sparklens)
  
- [dr-elephant](https://github.com/linkedin/dr-elephant)
