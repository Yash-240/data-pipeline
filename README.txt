Overview
This project implements a real-time data pipeline for monitoring, validating, processing, and storing sensor data into a relational database. The solution was created as part of the internship task for Bosch, focusing on building a scalable and efficient data pipeline to handle sensor data.

Files Overview:
monitor.py: Main script for monitoring and processing sensor data.
requirements.txt: Lists the Python dependencies required for the project.
README.md: Markdown file with project details (this file should be updated if you need detailed instructions).
internship_task_answer.docx: A detailed answer to the internship task (available in the docs/ folder).
data/: Directory containing any sample dataset files (e.g., CSVs or databases).
sensor_data.db: Example SQLite database to store processed sensor data.

Requirements:
Python 3.x
To install the necessary Python packages, use the requirements.txt:
pip install -r requirements.txt

How to Run the Project:
Install the dependencies:
pip install -r requirements.txt

Run the main Python script:
python monitor.py

Project Structure:
data-pipeline/
├── code/
│   └── monitor.py
├── data/
│   └── data_2017_07.csv
├── docs/
│   └── internship_task_solution.docx
├── requirements.txt
└── README.md

Notes:
This project assumes that the sensor data is available in CSV format and can be processed in real-time.
The data processing and transformation steps are handled in the monitor.py script.
The database is stored in SQLite (sensor_data.db) for simplicity.
This project is open-source and available under the MIT License.
