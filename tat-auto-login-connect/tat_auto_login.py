import subprocess
import json
import pyautogui
import pygetwindow as gw
import time
import pyperclip  # For clipboard usage

# Credentials (using environment variables is safer)
email = ""
password = ""

# Set this to 'YES' if you want to stop and restart TAT if it's already running
RESTART_TAT_IF_RUNNING = "NO"

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
            print(f"Error in PowerShell query: {result.stderr}")
            return None

        # Parse the JSON result to get the Package Family Name
        app_info = json.loads(result.stdout)
        
        if isinstance(app_info, list):
            app_info = app_info[0]  # If multiple apps are found, use the first one

        package_family_name = app_info.get('PackageFamilyName', None)
        if package_family_name:
            print(f"Package Family Name found: {package_family_name}")
            return package_family_name
        else:
            print("Package Family Name not found.")
            return None

    except Exception as e:
        print(f"Error retrieving the Package Family Name: {e}")
        return None

def is_tat_running():
    """
    Checks if the Trade Automation Toolbox (TAT) is currently running.
    
    Returns:
        bool: True if TAT is running, False otherwise.
    """
    try:
        # PowerShell command to check if the app is running
        command = 'powershell "Get-Process | Where-Object { $_.Name -like \'*TradeAutomationToolbox*\' }"'
        result = subprocess.run(command, capture_output=True, text=True, shell=True)
        return bool(result.stdout.strip())
    except Exception as e:
        print(f"Error checking if TAT is running: {e}")
        return False

def stop_uwp_app(app_name="Trade Automation Toolbox"):
    """
    Stops the running UWP app by killing its process.

    Args:
        app_name (str): The name of the UWP app to stop.
    """
    try:
        # PowerShell command to stop the process of the UWP app
        command = f'powershell "Get-Process | Where-Object {{ $_.Name -like \'*{app_name}*\' }} | Stop-Process"'
        result = subprocess.run(command, capture_output=True, text=True, shell=True)
        
        if result.returncode == 0:
            print(f"{app_name} stopped successfully.")
        else:
            print(f"Failed to stop {app_name}: {result.stderr}")
    except Exception as e:
        print(f"Error stopping the app: {e}")

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
            print(f"UWP app {package_family_name} successfully started!")
        except Exception as e:
            print(f"Error starting the UWP app: {e}")
    else:
        print("No Package Family Name provided.")

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
        print("Trade Automation Toolbox window not found.")
        return
    
    # Ensure the window is active
    if app_window.isMinimized:
        app_window.restore()
    app_window.activate()
    time.sleep(2)  # Additional wait time to ensure window activation

    if not app_window.isActive:
        print("Window is not active.")
        return
    
    print("Window successfully activated.")

    # 1. Press TAB 3 times to reach the email field
    for _ in range(3):
        pyautogui.press('tab')
        time.sleep(0.5)  # Small delay for each TAB

    # 2. Clear the email field and enter the email
    pyautogui.hotkey('ctrl', 'a')  # Select all
    pyautogui.press('backspace')  # Delete the content

    # 3. Enter the email address (split to insert '@')
    email_part1, email_part2 = email.split("@")
    pyautogui.typewrite(email_part1, interval=0.1)

    # 4. Insert the "@" symbol via clipboard
    pyperclip.copy("@")  # Copy "@" to clipboard
    pyautogui.hotkey('ctrl', 'v')  # Paste "@"

    # 5. Enter the remaining part of the email address
    pyautogui.typewrite(email_part2, interval=0.1)

    # 6. Press TAB to move to the password field and type the password
    pyautogui.press('tab')
    pyautogui.typewrite(password, interval=0.1)

    # 7. Press TAB to move to the "Sign In" button and press Enter
    pyautogui.press('tab')
    pyautogui.press('enter')

    print("Login completed.")

    # 8. Wait 5 seconds after login
    time.sleep(5)

    # 9. Press TAB twice to reach the "Connect to Broker" button and press Enter
    pyautogui.press('tab')
    pyautogui.press('tab')
    pyautogui.press('enter')

    print("Connection to broker established.")

if __name__ == "__main__":
    # Try to retrieve the Package Family Name of the "Trade Automation Toolbox"
    package_family_name = get_uwp_app_package_family_name("TradeAutomationToolbox")

    # Check if TAT is already running
    if is_tat_running():
        if RESTART_TAT_IF_RUNNING == "YES":
            print("TAT is running. Stopping it before restarting...")
            stop_uwp_app("Trade Automation Toolbox")
        else:
            print("TAT is already running. Exiting script.")
            exit()

    # Start the UWP app if the Package Family Name was found
    if package_family_name:
        start_uwp_app(package_family_name)

    # Wait for the app to start and then execute the automated login
    time.sleep(10)  # Ensure the app has loaded before proceeding
    login_to_tat(email, password)
