# bag_collection

A Python module for managing and extracting data from collections of ROS bag files.

## Features
- Recursively finds and indexes ROS bag files in a directory
- Lists available bag files and topics
- Extracts fields from messages across multiple bag files
- Prints message definitions for topics
- Supports filtering topics by regular expression

## Installation

This module requires Python 3 and the following dependencies:
- `rosbag` (ROS Python API)
- `numpy`


Install dependencies with pip (if available on PyPI):
```bash
pip install numpy
# rosbag is part of ROS and must be installed via your ROS distribution
```

To install this package in editable (development) mode, run from the root of this repository:
```bash
pip install -e .
```

## Usage

```
from bag_collection import bag_collection

# Initialize with the directory containing your .bag files
bc = bag_collection('/path/to/bag/files')

# List all bag files
bc.print_bag_files()

# Index the collection (find topics and message types)
bc.index_collection()

# Print all topics
bc.print_topics()

# Print topics matching a regex
bc.print_topics(regexp='camera')

# Extract a field from all bag files
timestamps, values = bc.get_field(topic='/my_topic', field='my_field')

# Print message definition for a topic
bc.print_message_def(topic='/my_topic')
```

## Methods
- `find_bag_files(path, extension='.bag')`: Recursively find bag files
- `print_bag_files()`: Print all found bag files
- `index_collection()`: Index topics and message types
- `print_topics(regexp=None)`: Print topics, optionally filtered by regex
- `get_field(topic, field)`: Extract a field from all bag files
- `print_message_def(topic)`: Print the message definition for a topic

## License

See [LICENSE](LICENSE) for details.
