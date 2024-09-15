# Trade Automation Toolbox - Auto Login Script

This script automates the process of logging into the Trade Automation Toolbox (TAT) and connecting it to your broker. It utilizes Python's automation libraries to mimic keyboard interactions, such as entering your credentials and navigating through the UI, ensuring that the TAT connects to your broker in an efficient and hands-off manner.

## Features

- Automatically launches the Trade Automation Toolbox (TAT).
- Simulates the keyboard inputs required to log in.
- Automatically connects the TAT to your broker after login.
- Uses clipboard for special characters (like `@`) to ensure consistent input.
- Adds configurable wait times for login and broker connection.

## Requirements

### Software
- **Python 3.x**: Ensure that Python is installed on your machine. You can download it from [python.org](https://www.python.org/).

### Python Dependencies

The script relies on the following Python libraries:
- `pyautogui`: For simulating keyboard and mouse actions.
- `pygetwindow`: To handle and control windows.
- `pyperclip`: To manage clipboard content (for inserting special characters like `@`).
- `subprocess`: For running system commands, such as PowerShell queries to retrieve UWP app info.
- `time`: For adding delays between actions.

### Installation

To install the required dependencies, run the following command in your terminal:

```bash
pip install pyautogui pygetwindow pyperclip
