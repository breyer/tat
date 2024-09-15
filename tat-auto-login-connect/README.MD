# Trade Automation Toolbox - Auto Login Script

This script automates the process of logging into the Trade Automation Toolbox (TAT) and connecting it to your broker. It utilizes Python's automation libraries to mimic keyboard interactions, such as entering your credentials and navigating through the UI, ensuring that the TAT connects to your broker in an efficient and hands-off manner.

## Features

- Automatically launches the Trade Automation Toolbox (TAT).
- Simulates the keyboard inputs required to log in.
- Automatically connects the TAT to your broker after login.
- Uses clipboard for special characters (like `@`) to ensure consistent input.
- Adds configurable wait times for login and broker connection.
- **New Feature**: Optional restart functionality—stop and restart TAT if it's already running.
- **Graceful Exit**: If TAT is already running and a restart is not needed, the script exits without performing any actions.

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
- `psutil`: For detecting and stopping the running instance of the `Trade Automation Toolbox.exe` process.
- `json`: For handling JSON data when interacting with PowerShell queries.

### Installation

To install the required dependencies, run the following command in your terminal:

```bash
pip install pyautogui pygetwindow pyperclip psutil json

### Usage

Running the Script
Before running the script, ensure you have the Trade Automation Toolbox installed on your machine.

Update the credentials: Modify the email and password variables at the top of the script with your login details. It's recommended to use environment variables for security instead of hardcoding sensitive information.

Control TAT Restarts:

By default, the script is configured to check if TAT is already running. If it is, the script will exit without taking any action.
You can control whether TAT should be restarted by modifying the RESTART_TAT_IF_RUNNING variable.
Set RESTART_TAT_IF_RUNNING to "YES" if you want the script to stop and restart TAT when it's already running.
Leave it as "NO" to avoid restarting the app.
