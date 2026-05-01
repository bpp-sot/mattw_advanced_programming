# BP0288990 Advanced Programming Assignment
# Shell Script Overview

This repository contains a modular Bash script designed to automate the backup of a PostgreSQL database. The script has been written with a strong emphasis on **modularity**, **parameterisation**, **portability**, and **robust error handling**, ensuring it can be reused across development, testing, and production environments with minimal modification. Below is a guide on how to use the script, inclduing it's key operations as well as detail on input parameters and how to run the script with example commands. The script also contains comments throughout explaining key pieces of functionality.

---

## 📌 What the Script Does

- Validates input parameters before any further execution
- Connects to a PostgreSQL database instance using parameters supplied via input parameters
- Performs a database dump using in-built `pg_dump` functionality  
- Stores the log output in a timestamped directory   

---

## ▶️ How to Run the Script

### **Prerequisites**
- Bash (v4+ recommended)  
- PostgreSQL v18 and associated tools installed (`pg_dump`, `psql`)  
- Appropriate database credentials

### Input Parameters
- -d <DATABASE> - Database on which to perform the snapshot - **Required**
- -v <VERSION> - Snapshot Version, to allow multiple to be taken on the same day - **Required**
- -u <USER> - The PostgreSQL user to connect using - **Required**
- -t <TYPE_BACKUP> - The type of backup to perform, CORE or FULL - **Required**
- -c <CLEARUP> - Y or N to run snapshot clear up - **Required**

### **Execution**
Run the script directly from the command line, sudo-ing as the postgres user:
Example command:
```bash
./make_postgres_backup.sh -d "ALPR" -v "v1" -u "MattAdmin" -t "CORE" -c "Y"
```
---

## 📌 Unit Testing Evidence
The script has been thoroughly unit tested, with functions verified in isolation before integration testing was performed to ensure correct program and data flow across components.

Unit Testing documentation can be found here:
[make_postgres_backup_unit_testing/PostgreSQL Backup Script Unit Testing.xlsx](https://github.com/bpp-sot/mattw_advanced_programming/blob/5d926efc68c7a721cef77340b6c35478ef600aac/make_postgres_backup_unit_testing/PostgreSQL%20Backup%20Script%20Unit%20Testing.xlsx)

Assoicated evidence can be found here:
[make_postgres_backup_unit_testing](https://github.com/bpp-sot/mattw_advanced_programming/tree/5d926efc68c7a721cef77340b6c35478ef600aac/make_postgres_backup_unit_testing)

