# Filtered Bag Player

Simple GUI tool for filtering ROS 2 bags around a trigger topic.

It lets you:
- find when a topic was published
- keep a few seconds before and after each event
- play the matching clips
- export the clips as ROS 2 bag folders

## Requirements

- ROS 2 with `rosbag2_py`
- `PyQt5`

`rosbag2_py` is provided by ROS 2, so make sure your ROS environment is sourced before running the tool.

## Setup

```bash
sudo apt install python3-pyqt5
```

or:

```bash
python3 -m pip install -r requirements.txt
```

## Run

```bash
source /opt/ros/$ROS_DISTRO/setup.bash
python3 filtered_bag_player.py
```
