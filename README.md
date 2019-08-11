# Trickl AssertJ JSON
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

An AssertJ fluent assertion wrapper around the Skyscreamer JSON comparison library

Installation
============

To install from Maven Central:

```xml
<dependency>
	<groupId>com.github.trickl</groupId>
	<artifactId>assertj-json</artifactId>
	<version>0.1.0-SNAPSHOT</version>
</dependency>
```

Example
==========

```
    assertThat(json("{\"age\":43, \"friend_ids\":[16, 23, 52]}"))
        .allowingExtraUnexpectedFields()
        .allowingAnyArrayOrdering()
        .isSameJsonAs("{\"friend_ids\":[52, 23, 16]}");
```

## Acknowledgments

AssertJ - http://joel-costigliola.github.io/assertj/

Skyscreamer JSON Library - https://github.com/skyscreamer/JSONassert
