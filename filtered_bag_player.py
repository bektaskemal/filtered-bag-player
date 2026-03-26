#!/usr/bin/env python3

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import pathlib
import shutil
import subprocess
import sys
import tempfile
import traceback
from typing import Callable
from typing import Sequence

import rosbag2_py


ProgressCallback = Callable[[str], None]


@dataclass(frozen=True)
class ClipWindow:
  index: int
  start_ns: int
  end_ns: int
  trigger_count: int
  first_trigger_ns: int
  last_trigger_ns: int

  @property
  def duration_ns(self) -> int:
    return self.end_ns - self.start_ns


@dataclass(frozen=True)
class ScanResult:
  bag_dir: pathlib.Path
  storage_id: str
  trigger_topic: str
  available_topics: list[str]
  bag_start_ns: int
  trigger_times: list[int]
  windows: list[ClipWindow]


def seconds_to_nanoseconds(seconds: float) -> int:
  return int(seconds * 1e9)


def format_time_ns(timestamp_ns: int) -> str:
  return datetime.fromtimestamp(timestamp_ns / 1e9).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def format_duration_ns(duration_ns: int) -> str:
  return f"{duration_ns / 1e9:.3f}s"


def slugify_name(value: str) -> str:
  sanitized = "".join(character if character.isalnum() else "_" for character in value)
  sanitized = "_".join(part for part in sanitized.split("_") if part)
  return sanitized.lower() or "filtered_bags"


def validate_bag_dir(bag_dir: pathlib.Path) -> None:
  if not (bag_dir / "metadata.yaml").exists():
    raise FileNotFoundError(f"Expected metadata.yaml in {bag_dir}")


def read_bag_metadata(bag_dir: pathlib.Path):
  validate_bag_dir(bag_dir)
  return rosbag2_py.MetadataIo().read_metadata(str(bag_dir))


def get_bag_topics(bag_dir: pathlib.Path) -> list[str]:
  metadata = read_bag_metadata(bag_dir)
  return sorted(topic.topic_metadata.name for topic in metadata.topics_with_message_count)


def validate_output_dir(out_dir: pathlib.Path, overwrite: bool) -> None:
  if out_dir.exists() and any(out_dir.iterdir()) and not overwrite:
    raise FileExistsError(
      f"Output directory {out_dir} already exists and is not empty. Use overwrite to reuse it.",
    )
  out_dir.mkdir(parents=True, exist_ok=True)


def make_export_dir(parent_dir: pathlib.Path, bag_dir: pathlib.Path, trigger_topic: str) -> pathlib.Path:
  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  bag_name = slugify_name(bag_dir.name)
  topic_name = slugify_name(trigger_topic)
  return parent_dir / f"{bag_name}_{topic_name}_{timestamp}"


def find_trigger_times(bag_uri: str, storage_id: str, topic: str) -> list[int]:
  reader = rosbag2_py.SequentialReader()
  storage_options = rosbag2_py.StorageOptions(uri=bag_uri, storage_id=storage_id)
  converter_options = rosbag2_py.ConverterOptions("", "")
  reader.open(storage_options, converter_options)
  reader.set_filter(rosbag2_py.StorageFilter(topics=[topic]))
  reader.set_read_order(rosbag2_py.ReadOrder(rosbag2_py.ReadOrderSortBy.File))

  trigger_times = []
  while reader.has_next():
    current_topic, _, timestamp_ns = reader.read_next()
    if current_topic == topic:
      trigger_times.append(timestamp_ns)

  return trigger_times


def merge_windows(trigger_times: list[int], before_ns: int, after_ns: int) -> list[ClipWindow]:
  if not trigger_times:
    return []

  sorted_triggers = sorted(trigger_times)
  merged_windows: list[ClipWindow] = []

  current_start_ns = max(0, sorted_triggers[0] - before_ns)
  current_end_ns = sorted_triggers[0] + after_ns
  current_count = 1
  current_first_trigger_ns = sorted_triggers[0]
  current_last_trigger_ns = sorted_triggers[0]

  for trigger_time_ns in sorted_triggers[1:]:
    start_ns = max(0, trigger_time_ns - before_ns)
    end_ns = trigger_time_ns + after_ns
    if start_ns <= current_end_ns:
      current_end_ns = max(current_end_ns, end_ns)
      current_count += 1
      current_last_trigger_ns = trigger_time_ns
      continue

    merged_windows.append(
      ClipWindow(
        index=len(merged_windows),
        start_ns=current_start_ns,
        end_ns=current_end_ns,
        trigger_count=current_count,
        first_trigger_ns=current_first_trigger_ns,
        last_trigger_ns=current_last_trigger_ns,
      ),
    )
    current_start_ns = start_ns
    current_end_ns = end_ns
    current_count = 1
    current_first_trigger_ns = trigger_time_ns
    current_last_trigger_ns = trigger_time_ns

  merged_windows.append(
    ClipWindow(
      index=len(merged_windows),
      start_ns=current_start_ns,
      end_ns=current_end_ns,
      trigger_count=current_count,
      first_trigger_ns=current_first_trigger_ns,
      last_trigger_ns=current_last_trigger_ns,
    ),
  )
  return merged_windows


def scan_bag(
  bag_dir: pathlib.Path,
  storage_id: str,
  topic: str,
  before_seconds: float,
  after_seconds: float,
  progress: ProgressCallback | None = None,
) -> ScanResult:
  validate_bag_dir(bag_dir)
  metadata = read_bag_metadata(bag_dir)
  resolved_storage_id = metadata.storage_identifier if storage_id == "auto" else storage_id
  if progress:
    progress(f"Scanning {bag_dir} with storage '{resolved_storage_id}'")

  available_topics = sorted(topic.topic_metadata.name for topic in metadata.topics_with_message_count)
  trigger_times = find_trigger_times(str(bag_dir), resolved_storage_id, topic)
  windows = merge_windows(
    trigger_times,
    seconds_to_nanoseconds(before_seconds),
    seconds_to_nanoseconds(after_seconds),
  )

  if progress:
    progress(f"Found {len(trigger_times)} trigger messages on {topic}")
    progress(f"Merged them into {len(windows)} clip(s)")

  return ScanResult(
    bag_dir=bag_dir,
    storage_id=resolved_storage_id,
    trigger_topic=topic,
    available_topics=available_topics,
    bag_start_ns=metadata.starting_time.nanoseconds,
    trigger_times=trigger_times,
    windows=windows,
  )


def yaml_list(items: Sequence[str], indent: int = 4) -> str:
  prefix = " " * indent
  return "\n".join(f"{prefix}- {item}" for item in items)


def build_output_yaml(
  output_bag_uri: str,
  storage_id: str,
  start_ns: int,
  end_ns: int,
  topics: Sequence[str] | None,
  include_services: bool,
) -> str:
  lines = [
    "output_bags:",
    f"- uri: {output_bag_uri}",
    f"  storage_id: {storage_id}",
    f"  start_time_ns: {start_ns}",
    f"  end_time_ns: {end_ns}",
  ]

  if topics is None:
    lines.append("  all_topics: true")
  else:
    lines.append("  topics:")
    lines.append(yaml_list(topics))

  lines.append(f"  all_services: {'true' if include_services else 'false'}")
  lines.append("  include_hidden_topics: true")
  return "\n".join(lines) + "\n"


def clip_dir_name(clip: ClipWindow, bag_start_ns: int) -> str:
  offset_seconds = max(0.0, (clip.start_ns - bag_start_ns) / 1e9)
  return f"start_{offset_seconds:.3f}s".replace(".", "_")


def run_convert(
  input_bag_uri: str,
  storage_id: str,
  clip: ClipWindow,
  output_clip_dir: pathlib.Path,
  topics: Sequence[str] | None,
  include_services: bool,
  progress: ProgressCallback | None = None,
) -> pathlib.Path:
  output_yaml = build_output_yaml(
    str(output_clip_dir),
    storage_id,
    clip.start_ns,
    clip.end_ns,
    topics,
    include_services,
  )

  with tempfile.NamedTemporaryFile(
    mode="w",
    suffix=".yaml",
    prefix=f"{output_clip_dir.name}_",
    delete=False,
    encoding="utf-8",
  ) as output_yaml_file:
    output_yaml_file.write(output_yaml)
    output_yaml_path = pathlib.Path(output_yaml_file.name)

  command = [
    "ros2",
    "bag",
    "convert",
    "-i",
    input_bag_uri,
    storage_id,
    "-o",
    str(output_yaml_path),
  ]
  if progress:
    progress(f"Running: {' '.join(command)}")

  try:
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
  finally:
    output_yaml_path.unlink(missing_ok=True)

  if progress:
    for stream in (completed.stdout, completed.stderr):
      for line in stream.splitlines():
        if line.strip():
          progress(line)

  if completed.returncode != 0:
    raise RuntimeError(
      f"ros2 bag convert failed for clip_{clip.index:03d} with exit code {completed.returncode}",
    )

  return output_clip_dir


def export_windows(
  bag_dir: pathlib.Path,
  storage_id: str,
  bag_start_ns: int,
  windows: Sequence[ClipWindow],
  out_dir: pathlib.Path,
  topics: Sequence[str] | None,
  include_services: bool,
  overwrite: bool,
  progress: ProgressCallback | None = None,
) -> list[pathlib.Path]:
  validate_output_dir(out_dir, overwrite)

  output_dirs = []
  for clip in windows:
    output_clip_dir = out_dir / clip_dir_name(clip, bag_start_ns)
    clip_dir = run_convert(
      input_bag_uri=str(bag_dir),
      storage_id=storage_id,
      clip=clip,
      output_clip_dir=output_clip_dir,
      topics=topics,
      include_services=include_services,
      progress=progress,
    )
    output_dirs.append(clip_dir)
    if progress:
      progress(f"Wrote {clip_dir}")

  return output_dirs


def copy_clip_dirs(
  source_dirs: Sequence[pathlib.Path],
  out_dir: pathlib.Path,
  overwrite: bool,
  progress: ProgressCallback | None = None,
) -> list[pathlib.Path]:
  validate_output_dir(out_dir, overwrite)

  copied_dirs = []
  for source_dir in source_dirs:
    destination_dir = out_dir / source_dir.name
    shutil.copytree(source_dir, destination_dir)
    copied_dirs.append(destination_dir)
    if progress:
      progress(f"Copied {destination_dir}")

  return copied_dirs


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
  arguments = list(sys.argv[1:] if argv is None else argv)
  default_to_gui = not arguments

  parser = argparse.ArgumentParser(
    description="Extract bag clips around every publish on a trigger topic.",
  )
  parser.add_argument("--gui", action="store_true", help="Launch the desktop GUI")
  parser.add_argument("bag", nargs="?", help="Path to the bag directory containing metadata.yaml")
  parser.add_argument("topic", nargs="?", help="Trigger topic, for example /vehicle/state")
  parser.add_argument(
    "--storage-id",
    default="auto",
    help="Storage backend, usually auto, mcap or sqlite3",
  )
  parser.add_argument(
    "--before",
    type=float,
    default=3.0,
    help="Seconds to keep before each trigger publish",
  )
  parser.add_argument(
    "--after",
    type=float,
    default=3.0,
    help="Seconds to keep after each trigger publish",
  )
  parser.add_argument(
    "--out-dir",
    default="filtered_clips",
    help="Directory that will receive exported clip folders",
  )
  parser.add_argument(
    "--topics",
    nargs="+",
    default=None,
    help="Optional explicit list of topics to keep in each output clip",
  )
  parser.add_argument(
    "--include-services",
    action="store_true",
    help="Include recorded service events in each output clip",
  )
  parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Allow writing into an existing output directory",
  )

  args = parser.parse_args(arguments)
  if default_to_gui:
    args.gui = True

  if not args.gui and (not args.bag or not args.topic):
    parser.error("bag and topic are required unless --gui is used")

  return args


def run_cli(args: argparse.Namespace) -> int:
  bag_dir = pathlib.Path(args.bag).resolve()
  out_dir = pathlib.Path(args.out_dir).resolve()

  try:
    scan_result = scan_bag(
      bag_dir=bag_dir,
      storage_id=args.storage_id,
      topic=args.topic,
      before_seconds=args.before,
      after_seconds=args.after,
      progress=print,
    )
  except FileNotFoundError as error:
    print(error, file=sys.stderr)
    return 2

  if not scan_result.trigger_times:
    print(f"No messages found on trigger topic {args.topic}", file=sys.stderr)
    return 1

  try:
    export_windows(
      bag_dir=scan_result.bag_dir,
      storage_id=scan_result.storage_id,
      bag_start_ns=scan_result.bag_start_ns,
      windows=scan_result.windows,
      out_dir=out_dir,
      topics=args.topics,
      include_services=args.include_services,
      overwrite=args.overwrite,
      progress=print,
    )
  except (FileExistsError, RuntimeError) as error:
    print(error, file=sys.stderr)
    return 2

  print("\nPlayback:")
  first_clip_name = clip_dir_name(scan_result.windows[0], scan_result.bag_start_ns)
  print(f"  ros2 bag play {out_dir}/{first_clip_name}")
  print("\nInspect one clip:")
  print(f"  ros2 bag info {out_dir}/{first_clip_name}")
  print("\nPlay all clips in order:")
  print(f"  for bag in {out_dir}/start_*; do ros2 bag play \"$bag\"; done")
  return 0


def run_gui() -> int:
  try:
    from PyQt5 import QtCore
    from PyQt5 import QtGui
    from PyQt5 import QtWidgets
  except ImportError as error:
    print(f"PyQt5 is required for GUI mode: {error}", file=sys.stderr)
    return 3

  class FunctionWorker(QtCore.QObject):
    finished = QtCore.pyqtSignal(object)
    failed = QtCore.pyqtSignal(str)

    def __init__(self, task: Callable[[ProgressCallback], object]) -> None:
      super().__init__()
      self.task_ = task

    @QtCore.pyqtSlot()
    def run(self) -> None:
      try:
        result = self.task_(lambda _: None)
      except Exception as error:
        self.failed.emit(str(error) or traceback.format_exc().strip())
      else:
        self.finished.emit(result)

  class DropZone(QtWidgets.QFrame):
    pathDropped = QtCore.pyqtSignal(str)
    clicked = QtCore.pyqtSignal()

    def __init__(self) -> None:
      super().__init__()
      self.setObjectName("dropZone")
      self.setAcceptDrops(True)
      self.setCursor(QtCore.Qt.PointingHandCursor)

      layout = QtWidgets.QVBoxLayout(self)
      layout.setContentsMargins(28, 28, 28, 28)
      layout.setSpacing(8)

      self.title_label_ = QtWidgets.QLabel("Drop or select a bag folder")
      self.title_label_.setObjectName("dropZoneTitle")
      self.title_label_.setAlignment(QtCore.Qt.AlignCenter)
      layout.addWidget(self.title_label_)

      self.subtitle_label_ = QtWidgets.QLabel("Click anywhere here to choose a folder")
      self.subtitle_label_.setObjectName("dropZoneSubtitle")
      self.subtitle_label_.setAlignment(QtCore.Qt.AlignCenter)
      layout.addWidget(self.subtitle_label_)

      self.path_label_ = QtWidgets.QLabel("")
      self.path_label_.setObjectName("dropZonePath")
      self.path_label_.setAlignment(QtCore.Qt.AlignCenter)
      self.path_label_.setWordWrap(True)
      layout.addWidget(self.path_label_)

    def setPath(self, path: pathlib.Path | None) -> None:
      if path is None:
        self.title_label_.setText("Drop or select a bag folder")
        self.subtitle_label_.setText("Click anywhere here to choose a folder")
        self.path_label_.setText("")
        return

      self.title_label_.setText(path.name)
      self.subtitle_label_.setText("Bag folder selected")
      self.path_label_.setText(str(path))

    def _extract_path(self, event) -> str | None:
      urls = event.mimeData().urls()
      if not urls:
        return None
      local_path = pathlib.Path(urls[0].toLocalFile())
      return str(local_path) if local_path.exists() else None

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
      if event.button() == QtCore.Qt.LeftButton:
        self.clicked.emit()
      super().mousePressEvent(event)

    def dragEnterEvent(self, event) -> None:  # type: ignore[override]
      local_path = self._extract_path(event)
      if local_path is not None and pathlib.Path(local_path).is_dir():
        event.acceptProposedAction()
        return
      event.ignore()

    def dropEvent(self, event) -> None:  # type: ignore[override]
      local_path = self._extract_path(event)
      if local_path is None:
        event.ignore()
        return
      self.pathDropped.emit(local_path)
      event.acceptProposedAction()

  class MainWindow(QtWidgets.QWidget):
    def __init__(self) -> None:
      super().__init__()
      self.setWindowTitle("Filtered Bag Player")
      self.resize(920, 640)

      self.bag_dir_: pathlib.Path | None = None
      self.scan_result_: ScanResult | None = None
      self.player_process_: subprocess.Popen[str] | None = None
      self.playback_queue_: list[pathlib.Path] = []
      self.playback_index_: int = -1
      self.playback_cache_key_: tuple[object, ...] | None = None
      self.playback_cache_dirs_: list[pathlib.Path] = []
      self.playback_timer_ = QtCore.QTimer(self)
      self.playback_timer_.setInterval(500)
      self.temp_play_roots_: list[pathlib.Path] = []
      self.worker_thread_: QtCore.QThread | None = None
      self.worker_: FunctionWorker | None = None

      self._build_ui()
      self._wire_signals()
      self._show_step(0)

    def _build_ui(self) -> None:
      outer_layout = QtWidgets.QVBoxLayout(self)
      outer_layout.setContentsMargins(28, 24, 28, 24)
      outer_layout.setSpacing(18)

      self.setObjectName("window")

      header_layout = QtWidgets.QVBoxLayout()
      header_layout.setSpacing(6)
      outer_layout.addLayout(header_layout)

      title_label = QtWidgets.QLabel("Filtered Bag Player")
      title_label.setObjectName("heroTitle")
      header_layout.addWidget(title_label)

      subtitle_label = QtWidgets.QLabel("Build short bag clips around trigger-topic events.")
      subtitle_label.setObjectName("pageSubtitle")
      header_layout.addWidget(subtitle_label)

      self.card_frame_ = QtWidgets.QFrame()
      self.card_frame_.setObjectName("card")
      card_layout = QtWidgets.QVBoxLayout(self.card_frame_)
      card_layout.setContentsMargins(26, 24, 26, 22)
      card_layout.setSpacing(18)
      outer_layout.addWidget(self.card_frame_, 1)

      self.stack_ = QtWidgets.QStackedWidget()
      card_layout.addWidget(self.stack_, 1)

      self._build_bag_page()
      self._build_options_page()
      self._build_results_page()

      self.status_label_ = QtWidgets.QLabel("Choose a bag folder to begin.")
      self.status_label_.setObjectName("statusLabel")
      outer_layout.addWidget(self.status_label_)

      self._apply_styles()

    def _build_bag_page(self) -> None:
      page = QtWidgets.QWidget()
      layout = QtWidgets.QVBoxLayout(page)
      layout.setSpacing(18)
      layout.addStretch(1)

      page_title = QtWidgets.QLabel("Choose a bag")
      page_title.setObjectName("pageTitle")
      page_title.setAlignment(QtCore.Qt.AlignCenter)
      layout.addWidget(page_title)

      page_subtitle = QtWidgets.QLabel("Drop a ROS 2 bag folder or click the area below.")
      page_subtitle.setObjectName("pageSubtitle")
      page_subtitle.setAlignment(QtCore.Qt.AlignCenter)
      layout.addWidget(page_subtitle)

      self.drop_zone_ = DropZone()
      self.drop_zone_.setMinimumHeight(220)
      layout.addWidget(self.drop_zone_)

      bag_actions_layout = QtWidgets.QHBoxLayout()
      bag_actions_layout.addStretch(1)
      self.continue_bag_button_ = QtWidgets.QPushButton("Continue")
      self.continue_bag_button_.setVisible(False)
      bag_actions_layout.addWidget(self.continue_bag_button_)
      bag_actions_layout.addStretch(1)
      layout.addLayout(bag_actions_layout)

      layout.addStretch(1)
      self.stack_.addWidget(page)

    def _build_options_page(self) -> None:
      page = QtWidgets.QWidget()
      layout = QtWidgets.QVBoxLayout(page)
      layout.setSpacing(18)

      top_layout = QtWidgets.QVBoxLayout()
      top_layout.setSpacing(6)
      layout.addLayout(top_layout)

      page_title = QtWidgets.QLabel("Configure the clip windows")
      page_title.setObjectName("pageTitle")
      top_layout.addWidget(page_title)

      self.options_bag_label_ = QtWidgets.QLabel("")
      self.options_bag_label_.setObjectName("bagLabel")
      self.options_bag_label_.setWordWrap(True)
      top_layout.addWidget(self.options_bag_label_)

      form_layout = QtWidgets.QGridLayout()
      form_layout.setHorizontalSpacing(16)
      form_layout.setVerticalSpacing(14)
      layout.addLayout(form_layout)

      row = 0
      form_layout.addWidget(QtWidgets.QLabel("Trigger topic"), row, 0)
      self.topic_combo_ = QtWidgets.QComboBox()
      self.topic_combo_.setObjectName("triggerTopicCombo")
      self.topic_combo_.setEditable(True)
      self.topic_combo_.setInsertPolicy(QtWidgets.QComboBox.NoInsert)
      self.topic_combo_.setMaxVisibleItems(20)
      self.topic_combo_.setCurrentIndex(-1)
      self.topic_combo_.lineEdit().setPlaceholderText("Select trigger topic")
      self.topic_completer_ = QtWidgets.QCompleter(self.topic_combo_)
      self.topic_completer_.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
      self.topic_completer_.setFilterMode(QtCore.Qt.MatchContains)
      self.topic_completer_.setCompletionMode(QtWidgets.QCompleter.PopupCompletion)
      self.topic_combo_.setCompleter(self.topic_completer_)
      form_layout.addWidget(self.topic_combo_, row, 1, 1, 3)

      row += 1
      form_layout.addWidget(QtWidgets.QLabel("Seconds before"), row, 0)
      self.before_spin_ = QtWidgets.QDoubleSpinBox()
      self.before_spin_.setRange(0.0, 3600.0)
      self.before_spin_.setDecimals(3)
      self.before_spin_.setValue(3.0)
      self.before_spin_.setButtonSymbols(QtWidgets.QAbstractSpinBox.NoButtons)
      form_layout.addWidget(self.before_spin_, row, 1)

      form_layout.addWidget(QtWidgets.QLabel("Seconds after"), row, 2)
      self.after_spin_ = QtWidgets.QDoubleSpinBox()
      self.after_spin_.setRange(0.0, 3600.0)
      self.after_spin_.setDecimals(3)
      self.after_spin_.setValue(3.0)
      self.after_spin_.setButtonSymbols(QtWidgets.QAbstractSpinBox.NoButtons)
      form_layout.addWidget(self.after_spin_, row, 3)

      topics_header_layout = QtWidgets.QHBoxLayout()
      topics_header_layout.addWidget(QtWidgets.QLabel("Included topics"))
      topics_header_layout.addStretch(1)
      self.select_all_topics_button_ = QtWidgets.QPushButton("Select All")
      self.select_all_topics_button_.setObjectName("secondaryButton")
      self.deselect_all_topics_button_ = QtWidgets.QPushButton("Deselect All")
      self.deselect_all_topics_button_.setObjectName("secondaryButton")
      topics_header_layout.addWidget(self.select_all_topics_button_)
      topics_header_layout.addWidget(self.deselect_all_topics_button_)
      layout.addLayout(topics_header_layout)

      self.topics_list_ = QtWidgets.QListWidget()
      self.topics_list_.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
      self.topics_list_.setMinimumHeight(220)
      layout.addWidget(self.topics_list_, 1)

      actions_layout = QtWidgets.QHBoxLayout()
      self.back_to_bag_button_ = QtWidgets.QPushButton("Back")
      self.back_to_bag_button_.setObjectName("secondaryButton")
      self.scan_button_ = QtWidgets.QPushButton("Scan")
      actions_layout.addWidget(self.back_to_bag_button_)
      actions_layout.addStretch(1)
      actions_layout.addWidget(self.scan_button_)
      layout.addLayout(actions_layout)

      self.stack_.addWidget(page)

    def _build_results_page(self) -> None:
      page = QtWidgets.QWidget()
      layout = QtWidgets.QVBoxLayout(page)
      layout.setSpacing(16)

      header_layout = QtWidgets.QVBoxLayout()
      header_layout.setSpacing(6)
      layout.addLayout(header_layout)

      page_title = QtWidgets.QLabel("Review and play")
      page_title.setObjectName("pageTitle")
      header_layout.addWidget(page_title)

      self.summary_label_ = QtWidgets.QLabel("No bag scanned yet.")
      self.summary_label_.setObjectName("summaryLabel")
      header_layout.addWidget(self.summary_label_)

      self.results_table_ = QtWidgets.QTableWidget(0, 4)
      self.results_table_.setHorizontalHeaderLabels(["Start", "End", "Duration", "Trigger count"])
      self.results_table_.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
      self.results_table_.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
      self.results_table_.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
      self.results_table_.setAlternatingRowColors(True)
      self.results_table_.verticalHeader().setDefaultSectionSize(36)
      self.results_table_.horizontalHeader().setStretchLastSection(True)
      self.results_table_.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)
      self.results_table_.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.Stretch)
      layout.addWidget(self.results_table_, 1)

      actions_layout = QtWidgets.QHBoxLayout()
      actions_layout.setSpacing(10)
      self.back_to_options_button_ = QtWidgets.QPushButton("Back")
      self.back_to_options_button_.setObjectName("secondaryButton")
      self.stop_playback_button_ = QtWidgets.QPushButton("Stop")
      self.stop_playback_button_.setObjectName("secondaryButton")
      self.play_selected_button_ = QtWidgets.QPushButton("Play Selected")
      self.export_selected_button_ = QtWidgets.QPushButton("Export Selected")
      self.export_all_button_ = QtWidgets.QPushButton("Export All")
      actions_layout.addWidget(self.back_to_options_button_)
      actions_layout.addStretch(1)
      actions_layout.addWidget(self.stop_playback_button_)
      actions_layout.addWidget(self.play_selected_button_)
      actions_layout.addWidget(self.export_selected_button_)
      actions_layout.addWidget(self.export_all_button_)
      layout.addLayout(actions_layout)

      self.export_selected_button_.setEnabled(False)
      self.export_all_button_.setEnabled(False)
      self.play_selected_button_.setEnabled(False)
      self.stop_playback_button_.setEnabled(False)

      self.stack_.addWidget(page)

    def _apply_styles(self) -> None:
      self.setStyleSheet(
        """
        QWidget#window {
          background: #f3efe7;
          color: #1b2a24;
        }
        QFrame#card {
          background: #fffaf2;
          border: 1px solid #e5ddd0;
          border-radius: 24px;
        }
        QLabel#heroTitle {
          font-size: 26px;
          font-weight: 700;
          letter-spacing: 0.3px;
          color: #17392f;
        }
        QLabel#pageTitle {
          font-size: 21px;
          font-weight: 650;
          color: #17392f;
        }
        QLabel#pageSubtitle, QLabel#hintLabel, QLabel#bagLabel {
          color: #66736c;
          font-size: 13px;
        }
        QLabel#summaryLabel {
          color: #4f5d56;
          font-size: 13px;
        }
        QLabel#statusLabel {
          color: #5e6f67;
          font-size: 12px;
          padding-left: 6px;
        }
        QLabel#dropZoneTitle {
          font-size: 20px;
          font-weight: 600;
          color: #17392f;
        }
        QLabel#dropZoneSubtitle, QLabel#dropZonePath {
          color: #5f6f68;
          font-size: 13px;
        }
        QFrame#dropZone {
          background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
            stop:0 #fcf7ef, stop:1 #f3ede3);
          border: 2px dashed #c9bda9;
          border-radius: 22px;
        }
        QFrame#dropZone:hover {
          border-color: #1f6b5c;
          background: #f7f3ea;
        }
        QPushButton {
          background: #1f6b5c;
          color: #fffaf3;
          border: none;
          border-radius: 14px;
          padding: 10px 16px;
          font-weight: 600;
        }
        QPushButton:hover {
          background: #275f53;
        }
        QPushButton:pressed {
          background: #16493d;
        }
        QPushButton:disabled {
          background: #d7d3cc;
          color: #8a8f8c;
        }
        QPushButton#secondaryButton {
          background: #ece5d9;
          color: #23483d;
        }
        QPushButton#secondaryButton:hover {
          background: #e0d8cb;
        }
        QLineEdit, QComboBox, QDoubleSpinBox, QListWidget, QTableWidget {
          background: #fffcf7;
          border: 1px solid #ddd3c5;
          border-radius: 14px;
          padding: 8px 10px;
        }
        QComboBox#triggerTopicCombo {
          padding-right: 34px;
        }
        QComboBox#triggerTopicCombo::drop-down {
          subcontrol-origin: padding;
          subcontrol-position: top right;
          width: 30px;
          border: none;
          background: transparent;
          margin-right: 8px;
        }
        QComboBox#triggerTopicCombo::down-arrow {
          image: none;
          width: 0px;
          height: 0px;
          border-left: 5px solid transparent;
          border-right: 5px solid transparent;
          border-top: 7px solid #5a6c64;
        }
        QListWidget, QTableWidget {
          padding: 6px;
        }
        QListWidget::item {
          padding: 8px 10px;
          border-radius: 10px;
        }
        QListWidget::item:selected {
          background: #d7eadf;
          color: #17392f;
        }
        QListWidget::indicator {
          width: 18px;
          height: 18px;
        }
        QListWidget::indicator:unchecked {
          border: 1px solid #b8b0a2;
          border-radius: 6px;
          background: #fffaf2;
        }
        QListWidget::indicator:checked {
          border: 1px solid #1f6b5c;
          border-radius: 6px;
          background: #1f6b5c;
        }
        QHeaderView::section {
          background: #efe6da;
          color: #4a5851;
          border: none;
          padding: 10px 8px;
          font-weight: 600;
        }
        QTableWidget {
          gridline-color: #efe6da;
          alternate-background-color: #faf5ee;
        }
        """
      )

    def _wire_signals(self) -> None:
      self.drop_zone_.clicked.connect(self._browse_bag)
      self.drop_zone_.pathDropped.connect(self._set_bag_dir_from_string)
      self.continue_bag_button_.clicked.connect(self._continue_with_selected_bag)
      self.back_to_bag_button_.clicked.connect(lambda: self._show_step(0))
      self.back_to_options_button_.clicked.connect(lambda: self._show_step(1))
      self.topic_combo_.currentTextChanged.connect(self._reorder_included_topics)
      self.topic_combo_.currentTextChanged.connect(self._update_scan_enabled)
      self.select_all_topics_button_.clicked.connect(self._check_all_topics)
      self.deselect_all_topics_button_.clicked.connect(self._uncheck_all_topics)
      self.topics_list_.itemChanged.connect(self._update_scan_enabled)
      self.scan_button_.clicked.connect(self._scan)
      self.export_selected_button_.clicked.connect(self._export_selected)
      self.export_all_button_.clicked.connect(self._export_all)
      self.play_selected_button_.clicked.connect(self._play_selected)
      self.stop_playback_button_.clicked.connect(self._stop_playback)
      self.playback_timer_.timeout.connect(self._check_playback_finished)

    def _browse_bag(self) -> None:
      default_dir = str(self.bag_dir_.parent if self.bag_dir_ is not None else pathlib.Path.home())
      selected = QtWidgets.QFileDialog.getExistingDirectory(
        self,
        "Select bag directory",
        default_dir,
      )
      if not selected:
        return
      self._set_bag_dir_from_string(selected)

    def _set_bag_dir_from_string(self, selected: str) -> None:
      try:
        self._set_bag_dir(pathlib.Path(selected).resolve())
      except Exception as error:
        self._set_status(f"Could not read bag: {error}")
        QtWidgets.QMessageBox.critical(self, "Could not open bag", str(error))

    def _set_bag_dir(self, bag_dir: pathlib.Path) -> None:
      metadata = read_bag_metadata(bag_dir)
      topics = sorted(topic.topic_metadata.name for topic in metadata.topics_with_message_count)

      self.bag_dir_ = bag_dir
      self.scan_result_ = None
      self._invalidate_playback_cache()
      self.drop_zone_.setPath(bag_dir)
      self.options_bag_label_.setText(str(bag_dir))
      self.summary_label_.setText("Run a scan to see the matching windows.")
      self.results_table_.setRowCount(0)
      self.stop_playback_button_.setEnabled(self.player_process_ is not None)
      self.continue_bag_button_.setVisible(True)
      self._populate_topics(topics)
      self.export_selected_button_.setEnabled(False)
      self.export_all_button_.setEnabled(False)
      self.play_selected_button_.setEnabled(False)
      self._set_status(f"Loaded {bag_dir.name} • {len(topics)} topics • {metadata.storage_identifier}")
      self._show_step(1)

    def _continue_with_selected_bag(self) -> None:
      if self.bag_dir_ is None:
        return
      self._show_step(1)

    def _populate_topics(self, topics: Sequence[str]) -> None:
      current_topic = self.topic_combo_.currentText().strip()
      checked_topics = set(self._topics_to_keep() or topics)
      self.topic_combo_.clear()
      self.topic_combo_.addItems(list(topics))
      self.topic_completer_.setModel(self.topic_combo_.model())
      if current_topic:
        index = self.topic_combo_.findText(current_topic)
        if index >= 0:
          self.topic_combo_.setCurrentIndex(index)
        else:
          self.topic_combo_.setEditText(current_topic)
      else:
        self.topic_combo_.setEditText("")
        self.topic_combo_.setCurrentIndex(-1)

      ordered_topics = list(topics)
      active_topic = self.topic_combo_.currentText().strip()
      if active_topic in ordered_topics:
        ordered_topics = [active_topic] + [topic for topic in ordered_topics if topic != active_topic]

      self.topics_list_.blockSignals(True)
      self.topics_list_.clear()
      for topic in ordered_topics:
        item = QtWidgets.QListWidgetItem(topic)
        item.setFlags(item.flags() | QtCore.Qt.ItemIsUserCheckable)
        item.setCheckState(QtCore.Qt.Checked if topic in checked_topics else QtCore.Qt.Unchecked)
        if topic == active_topic:
          font = item.font()
          font.setBold(True)
          item.setFont(font)
        self.topics_list_.addItem(item)
      self.topics_list_.blockSignals(False)
      self._update_scan_enabled()

    def _reorder_included_topics(self, active_topic: str | None = None) -> None:
      if self.topics_list_.count() == 0:
        return

      active_topic = (active_topic or self.topic_combo_.currentText()).strip()
      all_topics = [self.topic_combo_.itemText(index) for index in range(self.topic_combo_.count())]
      checked_topics = {
        self.topics_list_.item(index).text()
        for index in range(self.topics_list_.count())
        if self.topics_list_.item(index).checkState() == QtCore.Qt.Checked
      }

      ordered_topics = list(all_topics)
      if active_topic in ordered_topics:
        ordered_topics = [active_topic] + [topic for topic in ordered_topics if topic != active_topic]

      self.topics_list_.blockSignals(True)
      self.topics_list_.clear()
      for topic in ordered_topics:
        item = QtWidgets.QListWidgetItem(topic)
        item.setFlags(item.flags() | QtCore.Qt.ItemIsUserCheckable)
        item.setCheckState(QtCore.Qt.Checked if topic in checked_topics else QtCore.Qt.Unchecked)
        if topic == active_topic:
          font = item.font()
          font.setBold(True)
          item.setFont(font)
        self.topics_list_.addItem(item)
      self.topics_list_.blockSignals(False)
      self._update_scan_enabled()

    def _set_status(self, message: str) -> None:
      self.status_label_.setText(message)

    def _set_busy(self, busy: bool) -> None:
      for widget in [
        self.drop_zone_,
        self.continue_bag_button_,
        self.back_to_bag_button_,
        self.back_to_options_button_,
        self.select_all_topics_button_,
        self.deselect_all_topics_button_,
        self.scan_button_,
        self.export_selected_button_,
        self.export_all_button_,
        self.play_selected_button_,
      ]:
        widget.setEnabled(not busy)

      self.stop_playback_button_.setEnabled(self.player_process_ is not None)
      self.topic_combo_.setEnabled(not busy)
      self.before_spin_.setEnabled(not busy)
      self.after_spin_.setEnabled(not busy)
      self.topics_list_.setEnabled(not busy)
      if not busy:
        self.back_to_bag_button_.setEnabled(True)
        self.back_to_options_button_.setEnabled(True)
        self._update_scan_enabled()
        has_results = self.scan_result_ is not None and bool(self.scan_result_.windows)
        self.export_selected_button_.setEnabled(has_results)
        self.export_all_button_.setEnabled(has_results)
        self.play_selected_button_.setEnabled(has_results)

    def _show_step(self, index: int) -> None:
      self.stack_.setCurrentIndex(index)
      self.continue_bag_button_.setVisible(index == 0 and self.bag_dir_ is not None)
      if index == 1:
        QtCore.QTimer.singleShot(0, self.topic_combo_.setFocus)
      elif index == 2:
        QtCore.QTimer.singleShot(0, self.results_table_.setFocus)

    def _selected_windows(self) -> list[ClipWindow]:
      if self.scan_result_ is None:
        return []
      rows = sorted({index.row() for index in self.results_table_.selectionModel().selectedRows()})
      return [self.scan_result_.windows[row] for row in rows]

    def _topics_to_keep(self) -> list[str] | None:
      checked_topics = [
        self.topics_list_.item(index).text()
        for index in range(self.topics_list_.count())
        if self.topics_list_.item(index).checkState() == QtCore.Qt.Checked
      ]
      if len(checked_topics) == self.topics_list_.count():
        return None
      return checked_topics

    def _check_all_topics(self) -> None:
      self.topics_list_.blockSignals(True)
      for index in range(self.topics_list_.count()):
        self.topics_list_.item(index).setCheckState(QtCore.Qt.Checked)
      self.topics_list_.blockSignals(False)
      self._update_scan_enabled()

    def _uncheck_all_topics(self) -> None:
      self.topics_list_.blockSignals(True)
      for index in range(self.topics_list_.count()):
        self.topics_list_.item(index).setCheckState(QtCore.Qt.Unchecked)
      self.topics_list_.blockSignals(False)
      self._update_scan_enabled()

    def _has_included_topics(self) -> bool:
      return any(
        self.topics_list_.item(index).checkState() == QtCore.Qt.Checked
        for index in range(self.topics_list_.count())
      )

    def _has_valid_trigger_topic(self) -> bool:
      trigger_topic = self.topic_combo_.currentText().strip()
      if not trigger_topic:
        return False
      return self.topic_combo_.findText(trigger_topic, QtCore.Qt.MatchFixedString) >= 0

    def _update_scan_enabled(self) -> None:
      self.scan_button_.setEnabled(
        self.topics_list_.count() > 0 and self._has_included_topics() and self._has_valid_trigger_topic(),
      )

    def _ensure_included_topics(self) -> bool:
      if self.topics_list_.count() == 0:
        return False
      if self._topics_to_keep() == []:
        QtWidgets.QMessageBox.warning(self, "No topics selected", "Keep at least one topic enabled.")
        return False
      return True

    def _scan(self) -> None:
      topic = self.topic_combo_.currentText().strip()
      if self.bag_dir_ is None or not topic:
        QtWidgets.QMessageBox.warning(self, "Missing input", "Choose a bag and trigger topic first.")
        return
      if not self._has_valid_trigger_topic():
        QtWidgets.QMessageBox.warning(self, "Invalid trigger topic", "Choose a trigger topic from the bag topic list.")
        return
      if not self._ensure_included_topics():
        return

      before_seconds = self.before_spin_.value()
      after_seconds = self.after_spin_.value()

      def task(progress: ProgressCallback) -> ScanResult:
        return scan_bag(
          bag_dir=self.bag_dir_,
          storage_id="auto",
          topic=topic,
          before_seconds=before_seconds,
          after_seconds=after_seconds,
          progress=progress,
        )

      self._set_status("Scanning bag...")
      self._start_worker(task, self._scan_finished)

    def _scan_finished(self, result: ScanResult) -> None:
      self.scan_result_ = result
      self._invalidate_playback_cache()
      self._populate_topics(result.available_topics)
      self.topic_combo_.setCurrentText(result.trigger_topic)
      self._populate_table(result.windows)
      self.summary_label_.setText(
        f"{len(result.windows)} windows from {len(result.trigger_times)} trigger messages"
        f" • storage {result.storage_id}"
      )
      self._set_status(f"Found {len(result.windows)} matching windows.")
      has_results = bool(result.windows)
      self.export_selected_button_.setEnabled(has_results)
      self.export_all_button_.setEnabled(has_results)
      self.play_selected_button_.setEnabled(has_results)
      self._show_step(2)

    def _populate_table(self, windows: Sequence[ClipWindow]) -> None:
      self.results_table_.setRowCount(len(windows))
      for row, clip in enumerate(windows):
        self.results_table_.setVerticalHeaderItem(row, QtWidgets.QTableWidgetItem(str(clip.index + 1)))
        values = [
          format_time_ns(clip.start_ns),
          format_time_ns(clip.end_ns),
          format_duration_ns(clip.duration_ns),
          str(clip.trigger_count),
        ]
        for column, value in enumerate(values):
          self.results_table_.setItem(row, column, QtWidgets.QTableWidgetItem(value))
      self.results_table_.resizeRowsToContents()
      if windows:
        self.results_table_.selectRow(0)

    def _choose_export_parent_dir(self, title: str) -> pathlib.Path | None:
      default_dir = pathlib.Path.home()
      if self.bag_dir_ is not None:
        default_dir = self.bag_dir_.parent if self.bag_dir_.parent.exists() else default_dir

      selected = QtWidgets.QFileDialog.getExistingDirectory(
        self,
        title,
        str(default_dir),
      )
      return pathlib.Path(selected).resolve() if selected else None

    def _export_selected(self) -> None:
      selected_windows = self._selected_windows()
      if not selected_windows:
        QtWidgets.QMessageBox.warning(self, "No selection", "Select at least one clip to export.")
        return
      self._export_windows_dialog(selected_windows, "Select destination folder")

    def _export_all(self) -> None:
      if self.scan_result_ is None:
        return
      self._export_windows_dialog(self.scan_result_.windows, "Select destination folder")

    def _export_windows_dialog(self, windows: Sequence[ClipWindow], dialog_title: str) -> None:
      if self.scan_result_ is None:
        return
      if not self._ensure_included_topics():
        return

      parent_dir = self._choose_export_parent_dir(dialog_title)
      if parent_dir is None:
        return
      out_dir = make_export_dir(parent_dir, self.scan_result_.bag_dir, self.scan_result_.trigger_topic)

      topics = self._topics_to_keep()
      playback_key = self._playback_request_key(windows)

      if self._has_cached_playback(playback_key):
        def copy_task(progress: ProgressCallback) -> list[pathlib.Path]:
          return copy_clip_dirs(
            source_dirs=self.playback_cache_dirs_,
            out_dir=out_dir,
            overwrite=False,
            progress=progress,
          )

        self._set_status("Copying prepared clips...")
        self._start_worker(copy_task, lambda _: self._set_status(f"Exported clips to {out_dir}"))
        return

      def task(progress: ProgressCallback) -> list[pathlib.Path]:
        return export_windows(
          bag_dir=self.scan_result_.bag_dir,
          storage_id=self.scan_result_.storage_id,
          bag_start_ns=self.scan_result_.bag_start_ns,
          windows=windows,
          out_dir=out_dir,
          topics=topics,
          include_services=False,
          overwrite=False,
          progress=progress,
        )

      self._set_status("Exporting clips...")
      self._start_worker(task, lambda _: self._set_status(f"Exported clips to {out_dir}"))

    def _play_selected(self) -> None:
      selected_windows = self._selected_windows()
      if not selected_windows:
        QtWidgets.QMessageBox.warning(self, "No selection", "Select at least one clip to play.")
        return
      if self.scan_result_ is None:
        return
      if not self._ensure_included_topics():
        return
      playback_key = self._playback_request_key(selected_windows)
      if self._has_cached_playback(playback_key):
        self._set_status(f"Replaying {len(self.playback_cache_dirs_)} prepared clip(s)...")
        self._start_player(self.playback_cache_dirs_)
        return

      temp_root = pathlib.Path(tempfile.mkdtemp(prefix="filtered_bags_"))
      self.temp_play_roots_.append(temp_root)
      topics = self._topics_to_keep()

      def task(progress: ProgressCallback) -> list[pathlib.Path]:
        return export_windows(
          bag_dir=self.scan_result_.bag_dir,
          storage_id=self.scan_result_.storage_id,
          bag_start_ns=self.scan_result_.bag_start_ns,
          windows=selected_windows,
          out_dir=temp_root,
          topics=topics,
          include_services=False,
          overwrite=False,
          progress=progress,
        )

      self._set_status(f"Preparing {len(selected_windows)} clip(s) for playback...")
      self._start_worker(task, lambda clip_dirs: self._cache_and_start_player(playback_key, clip_dirs))

    def _start_player(self, clip_dirs: Sequence[pathlib.Path]) -> None:
      self._stop_playback(update_status=False)
      self.playback_queue_ = list(clip_dirs)
      self.playback_index_ = -1
      self._play_next_clip()

    def _cache_and_start_player(self, playback_key: tuple[object, ...], clip_dirs: object) -> None:
      resolved_clip_dirs = list(clip_dirs)
      self.playback_cache_key_ = playback_key
      self.playback_cache_dirs_ = resolved_clip_dirs
      self._start_player(resolved_clip_dirs)

    def _invalidate_playback_cache(self) -> None:
      self.playback_cache_key_ = None
      self.playback_cache_dirs_.clear()

    def _has_cached_playback(self, playback_key: tuple[object, ...]) -> bool:
      return (
        self.playback_cache_key_ == playback_key
        and bool(self.playback_cache_dirs_)
        and all((clip_dir / "metadata.yaml").exists() for clip_dir in self.playback_cache_dirs_)
      )

    def _playback_request_key(self, selected_windows: Sequence[ClipWindow]) -> tuple[object, ...]:
      if self.scan_result_ is None:
        raise RuntimeError("Playback requested before scan completed.")

      topics_to_keep = self._topics_to_keep()
      normalized_topics: tuple[str, ...] | None = None if topics_to_keep is None else tuple(sorted(topics_to_keep))
      selected_window_ids = tuple(window.index for window in selected_windows)
      return (
        str(self.scan_result_.bag_dir),
        self.scan_result_.storage_id,
        self.scan_result_.trigger_topic,
        self.scan_result_.bag_start_ns,
        tuple((window.start_ns, window.end_ns, window.trigger_count) for window in self.scan_result_.windows),
        selected_window_ids,
        normalized_topics,
      )

    def _play_next_clip(self) -> None:
      self.playback_index_ += 1
      if self.playback_index_ >= len(self.playback_queue_):
        self.playback_queue_.clear()
        self.playback_index_ = -1
        self.player_process_ = None
        self.playback_timer_.stop()
        self.stop_playback_button_.setEnabled(False)
        self._set_status("Finished playback.")
        return

      clip_dir = self.playback_queue_[self.playback_index_]
      command = ["ros2", "bag", "play", str(clip_dir)]
      try:
        self.player_process_ = subprocess.Popen(command)
      except OSError as error:
        self.playback_queue_.clear()
        self.playback_index_ = -1
        self.player_process_ = None
        self.playback_timer_.stop()
        self.stop_playback_button_.setEnabled(False)
        self._set_status(f"Could not start playback: {error}")
        return

      self._set_status(
        f"Playing clip {self.playback_index_ + 1}/{len(self.playback_queue_)}: {clip_dir.name}",
      )
      self.stop_playback_button_.setEnabled(True)
      self.playback_timer_.start()

    def _check_playback_finished(self) -> None:
      if self.player_process_ is None:
        self.playback_timer_.stop()
        return

      returncode = self.player_process_.poll()
      if returncode is None:
        return

      self.player_process_ = None
      if returncode == 0:
        self._play_next_clip()
      else:
        self.playback_timer_.stop()
        self.stop_playback_button_.setEnabled(False)
        self.playback_queue_.clear()
        self.playback_index_ = -1
        self._set_status(f"Playback exited with code {returncode}.")

    def _stop_playback(self, update_status: bool = True) -> None:
      self.playback_timer_.stop()
      if self.player_process_ is not None and self.player_process_.poll() is None:
        self.player_process_.terminate()
        try:
          self.player_process_.wait(timeout=3)
        except subprocess.TimeoutExpired:
          self.player_process_.kill()
      self.player_process_ = None
      self.playback_queue_.clear()
      self.playback_index_ = -1
      self.stop_playback_button_.setEnabled(False)
      if update_status:
        self._set_status("Playback stopped.")

    def _cleanup_temp_play_roots(self) -> None:
      for temp_root in self.temp_play_roots_:
        shutil.rmtree(temp_root, ignore_errors=True)
      self.temp_play_roots_.clear()

    def _start_worker(
      self,
      task: Callable[[ProgressCallback], object],
      on_finished: Callable[[object], None],
    ) -> None:
      if self.worker_thread_ is not None:
        QtWidgets.QMessageBox.information(self, "Busy", "Wait for the current task to finish.")
        return

      self.worker_thread_ = QtCore.QThread(self)
      self.worker_ = FunctionWorker(task)
      self.worker_.moveToThread(self.worker_thread_)
      self.worker_thread_.started.connect(self.worker_.run)
      self.worker_.finished.connect(on_finished)
      self.worker_.finished.connect(self._worker_done)
      self.worker_.failed.connect(self._worker_failed)
      self.worker_.failed.connect(self._worker_done)
      self.worker_thread_.start()
      self._set_busy(True)

    def _worker_failed(self, stack_trace: str) -> None:
      self._set_status("Operation failed.")
      QtWidgets.QMessageBox.critical(self, "Task failed", stack_trace)

    def _worker_done(self, _: object) -> None:
      if self.worker_thread_ is not None:
        self.worker_thread_.quit()
        self.worker_thread_.wait()
      if self.worker_ is not None:
        self.worker_.deleteLater()
      if self.worker_thread_ is not None:
        self.worker_thread_.deleteLater()
      self.worker_ = None
      self.worker_thread_ = None
      self._set_busy(False)

    def closeEvent(self, event) -> None:  # type: ignore[override]
      self._stop_playback(update_status=False)
      self._cleanup_temp_play_roots()
      super().closeEvent(event)

  app = QtWidgets.QApplication(sys.argv)
  app.setApplicationName("Filtered Bag Player")
  app.setFont(QtGui.QFont("Helvetica Neue", 11))
  window = MainWindow()
  window.show()
  return app.exec_()


def main(argv: Sequence[str] | None = None) -> int:
  args = parse_args(argv)
  if args.gui:
    return run_gui()
  return run_cli(args)


if __name__ == "__main__":
  raise SystemExit(main())
