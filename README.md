# FSBroker

FSBroker is a file system event broker library for Go. All the heavy lifting is thankfully done by [fsnotify](https://github.com/fsnotify/fsnotify).
While fsnotify is great, there are a few downsides which I find myself lacking in every project I use it in:

- I need to deduplicate events that happen in quick succession. An example would be the user repeatedly saving a file; I want to treat those bursts of file saves as an individual unit.
- FSNotify does not detect the "rename" properly, simply because operating systems handle renames in an obtuse way. For instance, on most operating systems, there will be a Create event (with the new name of the file) then a Rename event (with the old name of the file). It is difficult in my logic to correlate those two events together.
- I need to exclude common system files from raising events, such as ".DS_Store" on MacOS, or "thumbs.db" on Windows.
- I need to recursively add directories which are created in runtime to the watch list.
- I need to filter out some events based on path or type.

FSBroker allows you to watch for changes in the file system and handle events such as file creation, modification, and deletion.

## Features

- Deduplicate similar "Write" events which happen in quick succession and present them as a single unit
- Detect proper "Rename" events, providing the old and new paths (macOS and Linux only)
- Detect proper "Remove" events in cases where the file is moved (i.e. renamed) to a directory that is outside the watch directory
- Detect proper "Remove" events in which operating systems would emit as "Rename" because they are moved to a hidden trash directory
- Exclude common system files and directories
- Recursively add directories to the watch list
- Apply custom filters while pre-processing events

## Changelog

- (New x0.1.7) Update fsnotify to v1.9.0
- (New v0.1.6) Added the option to ignore hidden files.
- (New v0.1.5) Fix more bugs, added the option to emit chmod events (defaults to false).
- (New v0.1.4) Fix a bug where multiple consecutive file creations would not be detected on Windows.
- (New v0.1.4) Introduce the ability to interpret file moves/renames as deletes in cases where its appropriate.
- (New v0.1.3) Fix a problem where fsnotify would not detect file changes on macOS if the file was cleared.

## Installation

To install FS Broker, use `go get`:

```sh
go get github.com/helshabini/fsbroker
```

## Usage

Here is an example of how to use FS Broker:

```go
package main

import (
	"log"
	"time"

	"github.com/helshabini/fsbroker"
)

func main() {
	config := fsbroker.DefaultFSConfig()
	broker, err := fsbroker.NewFSBroker(config)
	if err != nil {
		log.Fatalf("error creating FS Broker: %v", err)
	}
	defer broker.Stop()

	if err := broker.AddRecursiveWatch("watch"); err != nil {
		log.Printf("error adding watch: %v", err)
	}

	broker.Start()

	for {
		select {
		case event := <-broker.Next():
			log.Printf("fs event has occurred: type=%s, path=%s, timestamp=%s, properties=%v", event.Type.String(), event.Path, event.Timestamp, event.Properties)
		case error := <-broker.Error():
			log.Printf("an error has occurred: %v", error)
		}
	}
}
```

You can also apply your own filters to events:

```go
broker.Filter = func(event *FSEvent) bool {
    return event.Type != Remove // Filters out any event that is not Remove
}
```

or

```go
broker.Filter = func(event *FSEvent) bool {
    return event.Path == "/some/excluded/path" // Filters out any event which is related to this path
}
```

## API

### `DefaultFSConfig() *FSConfig`

Creates a default FS Config instance with these default settings:

- Timeout:             300 * time.Millisecond
- IgnoreSysFiles:      true
- DarwinChmodAsModify: true
- EmitChmod:           false

### `NewFSBroker(config *FSConfig) (*FSBroker, error)`

Creates a new FS Broker instance.

- `config`: FS Config instance.

### `(*FSBroker) AddRecursiveWatch(path string) error`

Adds a recursive watch on the specified path.

- `path`: The directory path to watch.

### `(*FSBroker) Start()`

Starts the FS Broker to begin monitoring file system events.

### `(*FSBroker) Stop()`

Stops the FS Broker.

### `(*FSBroker) Next() <-chan *FSEvent`

Returns a channel that receives file system events.

### `(*FSBroker) Error() <-chan error`

Returns a channel that receives errors.

## Missing features:

Here is a list of features I would like to add in the future, please feel free to submit pull requests:

- Conditional recursion for directories

Currently, once you use FSBroker.AddRecursiveWatch it will automatically add newly created directories within any of the already watched directories to the watch list. Even if said watch directory was previously added using FSBroker.AddWatch. I would like to modify the watch map to allow for having the "recursivewatch" flag separately per watched directory, rather than globally on the entire broker.

- More comprehensive system file/directory detection

Currently, the list of system file/directory exclusion may not be 100% accurate. More testing and research on the detection process may be necessary.

- Separate the "FSBroker.Filter" function into two separate pre-filter and post-filter

Currently, FSBroker.Filter only runs while the event is being emitted out of FSNotify, before we apply any processing on it. I'd like to separate that into two different filter functions. One that runs as the event is added, and another as the event is being emitted to the user.

- Testing on different operating systems

I've only tested this on MacOS, I need someone to comprehensively test this on Linux and Windows.

## License

This project is licensed under the BSD-3-Clause License.
