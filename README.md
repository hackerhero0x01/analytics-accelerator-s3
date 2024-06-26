# S3 Connector Framework

This package hosts a set of shared primitives that can be used to build common connector functionality.

# Build system

The project is configured to be built via Gradle (Gradle 8.7). It also targets Java 8, as this is the most commonly used Java version in Data Lake stacks right now.

**Please be careful to not accidentally take a dependency on Java 11 and above - this is a one-way door** 

# Building
* To invoke a build and run tests: `./gradlew build`
* To skip reference tests: `./gradlew build -x referenceTest`
* To apply formatting: `./gradlew spotlessApply`
* To list all tasks: `./gradlew tasks`
* To publish JARs to Maven local: `./gradlew publishToMavenLocal`
* To build JMH benchmarks JAR: `./gradlew jmhJar`

# Microbenchmarks

We have a basic set of micro-benchmarks which test full sequential read, forward seeks, backward seeks and a 
Parquet-like ("jumping around") read pattern.

## Data Generation

Our JMH micro-benchmarks run against S3. To run the micro-benchmarks, you have to first generate data. To generate data 
you first have to tell us where to put this random data.  You can do this by populating the following two environment 
variables: `BENCHMARK_BUCKET` (the bucket you want to use) and `BENCHMARK_DATA_PREFIX` (you might want to use a common 
prefix in your bucket for all micro-benchmark related stuff). Now, to generate some random data, you can run `main` in 
`SequentialReadDataGenerator`.

## Running the Benchmarks

There are two ways:
1. Just run `./gradlew jmh --rerun`. (The reason for re-run is a Gradle-quirk. You may want to re-run benchmarks even when
you did not actually change the source of your project: `--rerun` turns off the Gradle optimisation that falls through
build steps when nothing changed.)
2. Run `java -jar input-stream/build/libs/input-stream-jmh.jar` (but don't forget to build the JMH JAR first; this you
can do with the `jmhJar` command listed above). This pattern is more useful when you want to add 
advanced configurations. For example, you could run `java -jar input-stream/build/libs/input-stream-jmh.jar -e SeekingReadBenchmarks`
to not run the benchmarks that seek.

## Developing integrations

When you are building this library into connectors, your IDE will need to be aware of the JARs (common, object-client,
input-stream). Consuming these via Maven/Gradle is natural, and you can use `./gradlew publishToMavenLocal` to have the
built JARs installed to your Maven local repository (this is `~/.m2` most of the time).


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.