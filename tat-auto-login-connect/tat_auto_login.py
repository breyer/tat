import subprocess
import pyautogui
import pygetwindow as gw
import time
import pyperclip  # For clipboard usage
import psutil  # To check if TAT is already running
import json
import os
import logging

logging.basicConfig(filename='tat_auto_login.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Credentials (using environment variables is safer)
email = os.getenv("EMAIL", "")
password = os.getenv("PASSWORD", "")

# Set this to 'YES' if you want to stop and restart TAT if it's already running
RESTART_TAT_IF_RUNNING = "YES"

def get_uwp_app_package_family_name(app_name="Trade Automation Toolbox"):
    """
    Retrieves the Package Family Name of a UWP app using PowerShell.

    Args:
        app_name (str): The display name of the UWP app.

    Returns:
        str: The Package Family Name of the UWP app, or None if not found.
    """
    try:
        # PowerShell command to get all installed UWP apps as JSON
        command = f'powershell "Get-AppxPackage | Where-Object {{ $_.Name -like \'*{app_name}*\' }} | ConvertTo-Json"'
        result = subprocess.run(command, capture_output=True, text=True, shell=True)
        
        if result.returncode != 0:
            logging.error(f"Error in PowerShell query: {result.stderr}")
            return None

        # Parse the JSON result to get the Package Family Name
        app_info = json.loads(result.stdout)
        
        if isinstance(app_info, list):
            app_info = app_info[0]  # If multiple apps are found, use the first one

        package_family_name = app_info.get('PackageFamilyName', None)
        if package_family_name:
            logging.info(f"Package Family Name found: {package_family_name}")
            return package_family_name
        else:
            logging.error("Package Family Name not found.")
            return None

    except Exception as e:
        logging.error(f"Error retrieving the Package Family Name: {e}")
        return None


def is_tat_running():
    """
    Checks if the Trade Automation Toolbox (TAT) executable is currently running.
    
    Returns:
        bool: True if TAT is running, False otherwise.
    """
    for process in psutil.process_iter(['pid', 'name']):
        if process.info['name'] == "Trade Automation Toolbox.exe":
            logging.info(f"TAT is running with PID: {process.info['pid']}")
            return True
    return False


def stop_tat_gracefully():
    """
    Attempts to gracefully stop the 'Trade Automation Toolbox.exe' process.
    Waits up to 15 seconds before forcefully killing the process if it doesn't exit.
    """
    for process in psutil.process_iter(['pid', 'name']):
        if process.info['name'] == "Trade Automation Toolbox.exe":
            logging.info(f"Attempting to gracefully terminate TAT with PID: {process.info['pid']}")
            process.terminate()  # Gracefully terminate the process

            # Wait up to 15 seconds for the process to terminate
            try:
                process.wait(15)  # Wait for up to 15 seconds
                logging.info("TAT terminated gracefully.")
            except psutil.TimeoutExpired:
                logging.warning("TAT did not terminate within 15 seconds, forcefully killing it.")
                process.kill()  # Force kill if it didn't terminate within 15 seconds
            except Exception as e:
                logging.error(f"Error while waiting for TAT to terminate: {e}")
                process.kill()  # Fallback to force kill in case of unexpected errors


def start_uwp_app(package_family_name):
    """
    Starts a UWP app using the Package Family Name.

    Args:
        package_family_name (str): The Package Family Name of the UWP app.
    """
    if package_family_name:
        try:
            # Start the UWP app using the Package Family Name
            subprocess.Popen(f"explorer.exe shell:AppsFolder\\{package_family_name}!App")
            logging.info(f"UWP app {package_family_name} successfully started!")
        except Exception as e:
            logging.error(f"Error starting the UWP app: {e}")
    else:
        logging.error("No Package Family Name provided.")


def login_to_tat(email, password):
    """
    Automates logging into the Trade Automation Toolbox.

    Args:
        email (str): The email for login.
        password (str): The password for login.
    """
    # Wait until the TAT window is visible
    time.sleep(10)  # Adjust if the program takes longer to load

    # Find and activate the TAT window
    try:
        app_window = gw.getWindowsWithTitle("Trade Automation Toolbox")[0]
    except IndexError:
        logging.error("Trade Automation Toolbox window not found.")
        return
    
    # Ensure the window is active
    if app_window.isMinimized:
        app_window.restore()
    app_window.activate()
    time.sleep(2)  # Additional wait time to ensure window activation

    if not app_window.isActive:
        logging.error("Window is not active.")
        return
    
    logging.info("Window successfully activated.")

    # 1. Press TAB 3 times to reach the email field
    for _ in range(3):
        pyautogui.press('tab')
        time.sleep(0.5)  # Small delay for each TAB

    # 2. Clear the email field by selecting all and deleting it
    pyautogui.hotkey('ctrl', 'a')  # Select all
    pyautogui.press('backspace')  # Delete the content

    # 3. Paste the entire email using clipboard
    pyperclip.copy(email)
    pyautogui.hotkey('ctrl', 'v')

    # 4. Press TAB to move to the password field
    pyautogui.press('tab')

    # 5. Clear the password field by selecting all and deleting it
    pyautogui.hotkey('ctrl', 'a')  # Select all
    pyautogui.press('backspace')  # Delete the content

    # 6. Paste the password using clipboard
    pyperclip.copy(password)
    pyautogui.hotkey('ctrl', 'v')

    # 7. Press Enter to log in
    pyautogui.press('enter')

    logging.info("Login completed.")

    # 8. Wait 5 seconds after login
    time.sleep(5)

    # 9. Press TAB twice to reach the "Connect to Broker" button and press Enter
    pyautogui.press('tab')
    pyautogui.press('tab')
    pyautogui.press('enter')

    logging.info("Connection to broker established.")


if __name__ == "__main__":
    # Try to retrieve the Package Family Name of the "Trade Automation Toolbox"
    package_family_name = get_uwp_app_package_family_name("TradeAutomationToolbox")

    # Check if TAT is already running
    if is_tat_running():
        if RESTART_TAT_IF_RUNNING == "YES":
            logging.info("TAT is running. Stopping it before restarting...")
            stop_tat_gracefully()
        else:
            logging.info("TAT is already running. Exiting script.")
            exit()

    # Start the UWP app if the Package Family Name was found
    if package_family_name:
        start_uwp_app(package_family_name)

    # Wait for the app to start and then execute the automated login
    time.sleep(10)  # Ensure the app has loaded before proceeding
    login_to_tat(email, password)
