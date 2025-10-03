# TAT Auto-Login & Connect

This script automates the process of launching, logging into, and connecting the Trade Automation Toolbox (TAT) to your broker. It uses UI automation to simulate keyboard and mouse interactions, saving you from the repetitive manual steps required each time you start the application.

## TAT compatible version
- **Tested** Version 4.0

## Features

- **Automatic Launch**: Starts the Trade Automation Toolbox (UWP app).
- **Automated Login**: Enters your email and password into the login form.
- **Broker Connection**: Clicks the "Connect to Broker" button after a successful login.
- **Clipboard for Reliability**: Uses the system clipboard to paste credentials, which can prevent issues with special characters.
- **Process Management**:
  - Checks if TAT is already running.
  - Optionally, it can gracefully stop a running instance before launching a new one.
- **Configurable**: Key settings can be adjusted at the top of the script or via environment variables.
- **Logging**: Provides detailed logs for troubleshooting, with an option to write to a file.

## Requirements

- **Python 3.x**: You can download it from [python.org](https://www.python.org/).
- **Trade Automation Toolbox**: The script assumes you have TAT installed on your machine.

### Python Dependencies

This script relies on several Python libraries. You can install them all with a single command:

```bash
pip install pyautogui pygetwindow pyperclip psutil
```

## How to Use

### 1. Configure Your Credentials

It is **strongly recommended** to use environment variables to keep your credentials secure.

- **For Windows**:
  ```powershell
  $env:EMAIL="your_email@example.com"
  $env:PASSWORD="your_password"
  ```
- **For macOS/Linux**:
  ```bash
  export EMAIL="your_email@example.com"
  export PASSWORD="your_password"
  ```

Alternatively, you can hardcode your credentials by editing these lines in the script, but this is less secure:
```python
email = os.getenv("EMAIL", "your_email@example.com")
password = os.getenv("PASSWORD", "your_password")
```

### 2. Configure Script Behavior

You can change the following variables at the top of the `tat_auto_login.py` script:

- `RESTART_TAT_IF_RUNNING`:
  - `"NO"` (default): The script will exit if it finds that TAT is already running.
  - `"YES"`: The script will stop the running TAT process and then restart it.
- `LOG_TO_FILE`:
  - `"NO"` (default): Logs are printed to the console.
  - `"YES"`: Logs are written to a file named `tat_auto_login.log` in the same directory.

### 3. Run the Script

Open your terminal or command prompt and run the script:

```bash
python tat_auto_login.py
```

The script will then perform the automated login and connection sequence.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
