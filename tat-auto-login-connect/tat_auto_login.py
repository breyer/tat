"""Automates the login process for the Trade Automation Toolbox (TAT) 4.0!

This script checks if the TAT application is running, starts it if it's not,
and then automates the UI interactions to log in with user credentials and
connect to the broker.
"""

import subprocess
import pyautogui
import pygetwindow as gw
import time
import pyperclip
import psutil
import json
import os
import logging

# --- Configuration Variables ---
# It is recommended to use environment variables for sensitive data.
email = os.getenv("EMAIL", "your_email@example.com")
password = os.getenv("PASSWORD", "your_password")

# Set to "YES" to automatically stop and restart TAT if it's already running.
RESTART_TAT_IF_RUNNING = "NO"

# Set to "YES" to log output to a file named 'tat_auto_login.log'.
LOG_TO_FILE = "NO"

# --- Logging Setup ---
if LOG_TO_FILE == "YES":
    logging.basicConfig(
        filename='tat_auto_login.log',
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
else:
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def get_uwp_app_package_family_name(app_name="Trade Automation Toolbox"):
    """Retrieves the Package Family Name for a UWP app via PowerShell.

    Args:
        app_name (str): The display name of the UWP application.

    Returns:
        str: The Package Family Name of the app, or None if not found.
    """
    try:
        command = (
            'powershell "Get-AppxPackage | '
            f"Where-Object {{ $_.Name -like '*{app_name}*' }} | ConvertTo-Json\""
        )
        result = subprocess.run(
            command, capture_output=True, text=True, shell=True
        )

        if result.returncode != 0:
            logging.error(f"Error in PowerShell query: {result.stderr}")
            return None

        app_info = json.loads(result.stdout)
        if isinstance(app_info, list):
            app_info = app_info[0]

        package_family_name = app_info.get('PackageFamilyName')
        if package_family_name:
            logging.info(f"Package Family Name found: {package_family_name}")
            return package_family_name
        else:
            logging.error("Package Family Name not found.")
            return None

    except Exception as e:
        logging.error(f"Error retrieving Package Family Name: {e}")
        return None


def is_tat_running():
    """Checks if the 'Trade Automation Toolbox.exe' process is running.

    Returns:
        bool: True if the process is running, False otherwise.
    """
    for process in psutil.process_iter(['pid', 'name']):
        if process.info['name'] == "Trade Automation Toolbox.exe":
            logging.info(f"TAT is running with PID: {process.info['pid']}")
            return True
    return False


def stop_tat_gracefully():
    """Attempts to gracefully terminate the TAT process.

    If the process does not terminate within 15 seconds, it is
    forcefully killed.
    """
    for process in psutil.process_iter(['pid', 'name']):
        if process.info['name'] == "Trade Automation Toolbox.exe":
            logging.info(
                "Attempting to gracefully terminate TAT with PID: "
                f"{process.info['pid']}"
            )
            process.terminate()
            try:
                process.wait(15)
                logging.info("TAT terminated gracefully.")
            except psutil.TimeoutExpired:
                logging.warning(
                    "TAT did not terminate in time, forcefully killing it."
                )
                process.kill()
            except Exception as e:
                logging.error(f"Error waiting for TAT to terminate: {e}")
                process.kill()


def start_uwp_app(package_family_name):
    """Starts a UWP application using its Package Family Name.

    Args:
        package_family_name (str): The Package Family Name of the UWP app.
    """
    if package_family_name:
        try:
            command = f"explorer.exe shell:AppsFolder\\{package_family_name}!App"
            subprocess.Popen(command)
            logging.info(f"UWP app '{package_family_name}' started.")
        except Exception as e:
            logging.error(f"Error starting the UWP app: {e}")
    else:
        logging.error("No Package Family Name provided to start_uwp_app.")


def login_to_tat(email, password):
    """Automates the UI interactions to log into TAT.

    This function waits for the TAT window to appear, activates it, and then
    simulates keyboard inputs to enter the email and password, log in, and

    Args:
        email (str): The email address for login.
        password (str): The password for login.
    """
    time.sleep(10)  # Wait for the app to load

    try:
        app_window = gw.getWindowsWithTitle("Trade Automation Toolbox")[0]
    except IndexError:
        logging.error("Trade Automation Toolbox window not found.")
        return

    if app_window.isMinimized:
        app_window.restore()
    app_window.activate()
    time.sleep(2)

    if not app_window.isActive:
        logging.error("TAT window could not be activated.")
        return

    logging.info("TAT window activated. Proceeding with login.")

    # Navigate to email field
    pyautogui.press('tab', presses=5, interval=0.5)

    # Enter email
    pyautogui.hotkey('ctrl', 'a')
    pyautogui.press('backspace')
    pyperclip.copy(email)
    pyautogui.hotkey('ctrl', 'v')

    # Navigate to password field
    pyautogui.press('tab')

    # Enter password
    pyautogui.hotkey('ctrl', 'a')
    pyautogui.press('backspace')
    pyperclip.copy(password)
    pyautogui.hotkey('ctrl', 'v')

    # Log in
    pyautogui.press('enter')
    logging.info("Login information submitted.")

    # Connect to broker
    time.sleep(5)
    pyautogui.press('tab', presses=3)
    pyautogui.press('enter')
    logging.info("Connection to broker initiated.")


if __name__ == "__main__":
    # Main execution block
    package_name = get_uwp_app_package_family_name("TradeAutomationToolbox")

    if is_tat_running():
        if RESTART_TAT_IF_RUNNING.upper() == "YES":
            logging.info("TAT is running. Stopping it before restarting.")
            stop_tat_gracefully()
        else:
            logging.info("TAT is already running. Exiting script.")
            exit()

    if package_name:
        start_uwp_app(package_name)
        login_to_tat(email, password)
    else:
        logging.error("Could not start TAT because Package Family Name was not found.")
