# S3 Connector Framework

This package hosts a set of shared primitives that can be used to build common connector functionality.

# Build system

The project is configured to be built via Gradle (Gradle 8.7). It also targets Java 8, as this is the most commonly used Java version in Data Lake stacks right now.

**Please be careful to not accidentally take a dependency on Java 11 and above - this is a one-way door** 

# Building
* To invoke a build and run tests: `./gradlew build`
* To skip tests: `./gradlew build -x test -x integrationTest`

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.