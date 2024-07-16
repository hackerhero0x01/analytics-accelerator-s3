export AWS_REGION=eu-west-1
export BENCHMARK_DATA_PREFIX=s3-connector-framework-benchmark
export BENCHMARK_BUCKET=gccsenge-microbenchmarks-dub

./gradlew jmhJar && java -jar input-stream/build/libs/input-stream-jmh.jar com.amazon.connector.s3.benchmark.SequentialReadBenchmark.testSequentialRead__with
