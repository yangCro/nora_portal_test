# nora_portal_test
scripts for nora portal test cases

## Installation

1. Clone the repository to local:
```
git clone https://github.com/yangCro/nora_portal_test.git
```

2. Install repository as python package and automatically install dependencies
```
cd nora_portal_test
python3 -m pip install -e .
```

## How to Run Scripts

Most scripts use `fire`, enabling calling functions as bash commands. And the repository is packaged as `nora`.

Example calling the script for `Task List A, 1st question`:
```
python3 -m nora.task_a1 run --mongo_ip {IP} --port {#PORT} --db_name {DB NAME} --collection_name {COLLECTION NAME}
```

## Scripts and Task Relation

Below are the scripts and task relationships:
```
nora/task_a1.py  # Task A, 1st bullet
nora/task_a3.py  # Task A, 3rd bullet
nora/task_optional.py  # Optional Task from D to G, named as "task_{index}" for function name
```
