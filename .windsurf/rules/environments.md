---
trigger: always
glob:
description: Related Java environments when run maven
---

# Environments list

JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

## Commands

### Install

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 mvn clean install -DskipTests
```

### Compile

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 mvn clean compile
```

### Copy dependencies

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 mvn dependency:copy-dependencies
```
