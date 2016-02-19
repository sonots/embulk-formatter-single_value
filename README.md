# An Embulk formatter plugin to output values of a single column

An Embulk formatter plugin to output values of a single column. This is a kind of reverse of [embulk-parser-none](https://github.com/sonots/embulk-parser-none)

## Overview

* **Plugin type**: formatter

## Configuration

* **message_key**: A column name which this plugin outputs (string, default: null which extracts the first column)
* **null_string**: A string value to express NULL value (string, default: "")
* **timestamp_format**: Timestamp format for timestamp column (string, default: "%Y-%m-%d %H:%M:%S.%6N %z")
* **timezone**: Timezone for timesatmp column (string, default: UTC)

## Example

```yaml
out:
  type: an output plugin supporting a formatter plugin such as `file`
  formatter:
    type: single_value
    message_key: message
    null_string: ""
```

See [./example](./example) for more.

## Changelog

[CHANGELOG.md](./CHANGELOG.md)

## Development

Run example:

```
# embulk gem install embulk-parser-none
$ ./gradlew classpath
$ embulk run -I lib example/example.yml
```

Run test:

```
$ ./gradlew test
```

Release gem:

```
$ ./gradlew gemPush
```
