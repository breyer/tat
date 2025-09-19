# Trading Analysis and Automation Tools

Welcome to the Trading Analysis and Automation Tools repository. This project is a collection of Python-based utilities designed to supplement the **Trade Automation Toolbox (TAT)**. These tools help with analyzing trade data, visualizing performance, and automating repetitive tasks like logging in and managing trade plans.

![Example PnL Plot](https://github.com/breyer/tat/blob/main/plot-example.png?raw=true)

## What's Inside?

This repository is organized into several key components, each with its own specific purpose and detailed documentation.

| Component                                     | Description                                                                                                                            |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| 📂 [**pnl**](./pnl/)                          | Scripts to visualize Profit and Loss (PnL) data from your TAT database. Includes a static HTML report and a live-updating web dashboard. |
| 📂 [**tradeplan2db3**](./tradeplan2db3/)      | A powerful command-line tool to update your TAT database from a `tradeplan.csv` file. Automates the setup of trade templates and schedules. |
| 📂 [**tat-auto-login-connect**](./tat-auto-login-connect/) | A script to automate the process of launching TAT, logging in with your credentials, and connecting to your broker.                   |
| 📂 [**ipynb**](./ipynb/)                      | A Jupyter Notebook for in-depth analysis of trading data, featuring heatmap visualizations to identify profitable trading patterns.        |

## Getting Started

### Prerequisites

- **Python 3.x**: Make sure you have a recent version of Python installed. You can download it from [python.org](https://www.python.org/).
- **Trade Automation Toolbox**: These tools are designed to work with the data and applications from TAT.

### Installation

Each component has its own specific dependencies. Please refer to the `README.md` file within each component's directory for detailed installation instructions.

A common first step is to clone this repository to your local machine:
```bash
git clone https://github.com/breyer/tat.git
cd tat
```

From there, navigate to the directory of the tool you wish to use and follow the setup instructions in its local `README.md`.

## How to Use

Each tool is designed to be run independently. For detailed usage instructions, please see the `README.md` file in the corresponding directory:

- **For PnL visualization**: [pnl/README.MD](./pnl/README.MD)
- **For updating the trade plan**: [tradeplan2db3/readme.md](./tradeplan2db3/readme.md)
- **For auto-login**: [tat-auto-login-connect/README.MD](./tat-auto-login-connect/README.MD)
- **For data analysis**: [ipynb/readme.md](./ipynb/readme.md)

## Contributing

Contributions are welcome! If you have ideas for new tools, improvements to existing ones, or have found a bug, please feel free to open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
