#!/bin/bash
set -euo pipefail

#
# Function Declarations
#

usage() {
  echo "ERROR : ${PROGNAME} : usage:"
  echo "   ${PROGNAME} [-d <DATABASE>] [-v <VERSION>] [-u <USER>] [-t <TYPE_BACKUP>] [-c <CLEARUP>]"
  echo "     -d <DATABASE>          : Mandatory, Database on which to perform the Snapshot"
  echo "     -v <VERSION>           : Mandatory, Snapshot Version"
  echo "     -u <USER>              : Mandatory, PostgreSQL User to connect using"
  echo "     -t <TYPE_BACKUP>       : Mandatory, Type of backup to perform, CORE, CORE_SELECT, FULL"
  echo "     -c <CLEARUP>           : Mandatory, Y or N to run Snapshot Clear Up"

  return 3
}

#------------------------------------------------------------------------------
# Function: runDatabaseSnapshotClearUp
#
# Purpose:
#   Implements the snapshot retention policy for PostgreSQL backups by:
#     - Ensuring a minimum number of backups are always retained (>= 3)
#     - Identifying backup directories older than the configured retention
#       threshold (default: 10 days)
#     - Deleting only those directories that exceed both conditions
#
# Behaviour:
#   - Validates that BACKUP_FOLDER exists
#   - Counts immediate subdirectories (each representing a single backup)
#   - If 3 or fewer backups exist, no deletion is performed (safety rule)
#   - Otherwise, finds backups older than $days_old and deletes them
#
# Safety Guarantees:
#   - Uses -mindepth 1 and -maxdepth 1 to ensure only immediate subdirectories
#     are considered, preventing accidental deletion of the root folder or
#     nested content.
#   - Uses explicit directory matching (-type d) to avoid file deletion.
#
# Returns:
#   0 on success, 1 on validation failure.
#------------------------------------------------------------------------------
function runDatabaseSnapshotClearUp
{
  #
  # Run Database Snapshot Clear Up
  #

  echo ""
  echo "#--------------------------------------"
  echo "#  Running Database Snapshot Clear Up  "
  echo "#--------------------------------------"
  echo ""

  # Local Variables	
  local days_old=10
  local old_files
  local preview_backups
  local file_count

  # Validate directory exists
  if [[ ! -d "${BACKUP_FOLDER}" ]]; then
      echo "ERROR: Directory '${BACKUP_FOLDER}' does not exist."
      return 1
  else
      echo "Directory '${BACKUP_FOLDER}' exists"
  fi

  # Count total subdirectories in directory
  # -type d sets the find operator to look for directories rather than -f which searches for files
  file_count=$(find "$BACKUP_FOLDER" -mindepth 1 -type d | wc -l)

  echo "Found $file_count backups in '${BACKUP_FOLDER}'."

  # Rule: If 3 or fewer files exist ’ do NOT delete anything
  if (( file_count <= 3 )); then
      # Function exits at this point
      echo "Only $file_count files present. No Clearup performed."
      echo "#-----------------------------"
      echo "# Snapshot Clearup Completed"
      echo "#-----------------------------"
      return 0   
  fi

  echo "Scanning for files older than $days_old days..."

  # Find files older than 10 days
  old_files=$(find "$BACKUP_FOLDER" -maxdepth 1 -mindepth 1 -type d -mtime +"$days_old" | wc -l)

  echo "Found $old_files backups in '${BACKUP_FOLDER}' older than $days_old."
  
  if (( old_files == 0 )); then
      # Function exits at this point
      echo "No files older than $days_old days found."
      echo "#-----------------------------"
      echo "# Snapshot Clearup Completed"
      echo "#-----------------------------"
      return 0
  fi

  echo "Deleting files older than $days_old days:"
  
  # Directories in scope of deletion
  preview_backups=$(find "$BACKUP_FOLDER" -mindepth 1 -maxdepth 1 -type d -mtime +"$days_old")
  echo "Backups in scope of deletion $preview_backups."

  # Delete directories in scope	& log what's being deleted
  find "$BACKUP_FOLDER" -mindepth 1 -maxdepth 1 -type d -mtime +"$days_old" \
    -exec sh -c 'echo "Deleting: $1"; rm -rf "$1"' _ {} \;

  echo "#-----------------------------"
  echo "# Snapshot Clearup Completed"
  echo "#-----------------------------"
  return 0
}

#------------------------------------------------------------------------------
# Function: runDatabaseSnapshotFull
#
# Purpose:
#   Performs a full PostgreSQL snapshot backup consisting of:
#     1. A global roles and database-wide configuration export (pg_dumpall --globals-only)
#     2. A full logical dump of the specified database using pg_dump
#
# Rationale:
#   PostgreSQL stores roles, permissions, and certain global objects outside
#   individual databases. A complete snapshot therefore requires:
#     - A globals dump (roles, tablespaces, privileges)
#     - A per-database dump (schema + data)
#   Combining both ensures the snapshot is fully restorable on a clean instance.
#
# Behaviour:
#   - Executes pg_dumpall to capture global metadata
#   - Executes pg_dump to capture the full logical state of $DATABASE
#   - Writes both outputs into $OUTDIR
#   - Applies permissive file permissions (chmod 777) to ensure downstream
#     files are accessible
#
# Requirements:
#   - Must be executed by a PostgreSQL superuser or a role with sufficient
#     privileges to read global metadata and all database objects.
#   - Assumes localhost connectivity and environment variables:
#       $USER        PostgreSQL role used for authentication
#       $DATABASE    Target database name
#       $TYPE_BACKUP Label for the backup type (e.g., FULL, CORE)
#       $OUTDIR      Output directory for generated dump files
#
# Output:
#   - roles_globals.sql
#   - <database>_<type>_snapshot.sql
#
# Returns:
#   0 on success.
#------------------------------------------------------------------------------
function runDatabaseSnapshotFull
{

  #
  # Run Database Snapshot Full Procedure
  #

  echo ""
  echo "#----------------------------------"
  echo "#  Running Database Snapshot Full  "
  echo "#----------------------------------"
  echo ""

  # 0) Roles & globals (run as superuser to avoid peer issues)
  pg_dumpall -U $USER --host localhost --globals-only > "${OUTDIR}/roles_globals.sql"

  # 1) Full Database Dump
  pg_dump -U "$USER" -h localhost -d "$DATABASE" -F p > "${OUTDIR}/${DATABASE}_${TYPE_BACKUP}_snapshot.sql"

  chmod 777 "${OUTDIR}/${DATABASE}_${TYPE_BACKUP}_snapshot.sql"
  chmod 777 "${OUTDIR}/roles_globals.sql"

  echo "Backup created in: ${OUTDIR}"
  echo " - roles_globals.sql"
  echo " - ${DATABASE}_${TYPE_BACKUP}_snapshot.sql (combined)"

  return 0
}

#------------------------------------------------------------------------------
# Function: runDatabaseSnapshotCore
#
# Purpose:
#   Generates a *core* PostgreSQL snapshot consisting of:
#     1. Global roles and cluster-wide metadata
#     2. Schema-only dump (pre‑data and post‑data sections)
#     3. Data-only dump for a curated subset of “core” tables defined in
#        unicdc.backup_config (where vpd_only = 'Y')
#     4. A final combined SQL file assembled in the correct restore order
#
# Rationale:
#   Some environments require a lightweight snapshot that captures only the
#   essential business‑critical tables rather than the full database. This
#   function implements a selective backup strategy by:
#     - Querying a configuration table to determine which tables qualify
#       as “core”
#     - Dumping only those tables’ data
#     - Preserving the full schema structure to ensure referential integrity
#   This approach reduces storage footprint and accelerates restore times
#   while maintaining logical consistency.
#
# Behaviour:
#   - Queries unicdc.backup_config to dynamically construct a list of tables
#     to include in the snapshot (formatted as repeated -t arguments for pg_dump)
#   - Exports global roles and metadata using pg_dumpall --globals-only
#   - Extracts schema definitions in two phases:
#       * pre-data  (DDL: types, tables, sequences)
#       * post-data (constraints, indexes, triggers)
#   - Dumps data only for the selected core tables
#   - Concatenates the three components into a single, ordered SQL file
#
# Requirements:
#   - Must be executed by a PostgreSQL superuser or a role with privileges to:
#       * Query unicdc.backup_config
#       * Read all schema objects
#       * Export global metadata
#   - Assumes localhost connectivity and environment variables:
#       $USER        PostgreSQL role used for authentication
#       $DATABASE    Target database name
#       $TYPE_BACKUP Label for the backup type (e.g., FULL, CORE)
#       $OUTDIR      Output directory for generated dump files
#
# Output:
#   - roles_globals.sql
#   - 00_schema_pre.sql
#   - 01_selected_data.sql
#   - 02_schema_post.sql
#   - <database>_<type>_snapshot.sql (final combined snapshot)
#
# Returns:
#   0 on success.
#------------------------------------------------------------------------------
function runDatabaseSnapshotCore
{

  #
  # Run Database Snapshot Core Procedure
  #

  echo ""
  echo "#---------------------------------"
  echo "#  Running Database Snapshot Core "
  echo "#---------------------------------"
  echo ""

  # Retrieve List of Core Tables to be retained

  echo ""
  echo "#---------------------------------"
  echo "#  Gathering Core Table List "
  echo "#---------------------------------"
  echo ""

  CORE_TAB_LIST=$(
    psql -U "$USER" -h localhost -d "$DATABASE" \
         -t -A -c "
      SELECT ' -t ' || string_agg(b.schema_name || '.' || b.table_name, ' -t ')
      FROM unicdc.backup_config b
      WHERE b.vpd_only = 'Y';
    " | xargs
  )
  
  echo "$CORE_TAB_LIST"

  # 0) Roles & globals (run as superuser to avoid peer issues)
  pg_dumpall -U $USER --host localhost --globals-only > "${OUTDIR}/roles_globals.sql"

  # 1) Schema: pre-data and post-data
  pg_dump -U $USER -h localhost -d "$DATABASE" --section=pre-data  -F p > "${OUTDIR}/00_schema_pre.sql"
  pg_dump -U $USER -h localhost -d "$DATABASE" --section=post-data -F p > "${OUTDIR}/02_schema_post.sql"

  # 2) Selected tables data (INSERTs)
  pg_dump -U $USER -h localhost -d "$DATABASE" --data-only --inserts \
    $CORE_TAB_LIST \
    -F p > "${OUTDIR}/01_selected_data.sql"

  # 3) Final combined SQL in correct order
  cat "${OUTDIR}/00_schema_pre.sql" \
      "${OUTDIR}/01_selected_data.sql" \
      "${OUTDIR}/02_schema_post.sql" \
      > "${OUTDIR}/${DATABASE}_${TYPE_BACKUP}_snapshot.sql"
      
  # Chmod 777 to ensure files are accessible
  chmod 777 "${OUTDIR}/${DATABASE}_${TYPE_BACKUP}_snapshot.sql"
  chmod 777 "${OUTDIR}/roles_globals.sql"

  echo "Backup created in: ${OUTDIR}"
  echo " - roles_globals.sql"
  echo " - ${DATABASE}_${TYPE_BACKUP}_snapshot.sql (combined)"

  return 0
}

#------------------------------------------------------------------------------
# Function: validateInputParams
#
# Purpose:
#   Performs defensive validation of all user‑supplied input parameters before
#   any backup operation is executed. This ensures that:
#     - Required parameters are present and correctly formatted
#     - Only supported backup modes are executed
#     - Invalid or malformed inputs are detected early, preventing runtime
#       failures, unsafe operations, or unintended database actions
#
# Rationale:
#   Shell scripts lack strong typing and are vulnerable to malformed input,
#   accidental misuse, and injection risks. Centralising validation in a
#   dedicated function provides:
#     - A single point of enforcement for input correctness
#     - Clear error reporting for end‑users
#     - A proactive error‑handling mechanism that prevents invalid state from
#       propagating into the backup workflow
#
# Behaviour:
#   - Validates each required parameter using regular expressions or controlled
#     enumerations
#   - Accumulates validation errors rather than failing fast, allowing the user
#     to see all issues in one pass
#   - Returns a non‑zero exit code (20) if any validation rule fails
#   - Returns 0 only when all parameters pass validation
#
# Validation Rules:
#   DATABASE:
#       - Must contain only alphanumeric characters, dots, underscores, or hyphens and must be populated
#   USER:
#       - Must contain only alphanumeric characters, dots, underscores, or hyphens and must be populated
#   VERSION:
#       - Must follow the pattern v<number> (e.g., v1, v2, v10)
#   TYPE_BACKUP:
#       - Must be one of: CORE or FULL
#   CLEARUP:
#       - Must be Y or N (case‑insensitive)
#
# Requirements:
#   Assumes the following environment variables are set prior to invocation:
#       $DATABASE
#       $USER
#       $VERSION
#       $TYPE_BACKUP
#       $CLEARUP
#
# Returns:
#   0   if all parameters are valid
#   20  if one or more validation checks fail
#------------------------------------------------------------------------------
function validateInputParams
{

  # 
  # Validate Users Input Parameters
  #

  echo ""
  echo "#-----------------------------"
  echo "#  Validating Input Parameters"
  echo "#-----------------------------"
  echo ""

  ERROR=0   # Variable to track validation failures

  # DATABASE: must be a non-empty string
  if [[ ! "$DATABASE" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "ERROR: Database name is invalid. Must be a string with letters, numbers, dot, underscore, or hyphen."
      ERROR=1
  fi

  # USER: must be a non-empty string
  if [[ ! "$USER" =~ ^[A-Za-z0-9._-]+$ ]]; then
      echo "ERROR: User is invalid. Must be a string with letters, numbers, dot, underscore, or hyphen."
      ERROR=1
  fi

  # VERSION: must look like v1, v2, v10 etc.
  if [[ ! "$VERSION" =~ ^v[0-9]+$ ]]; then
      echo "ERROR: Version must be in the format v1, v2, v10, etc."
      ERROR=1
  fi

  # TYPE_BACKUP: must be CORE, CORE_SELECT, FULL
  case "$TYPE_BACKUP" in
      CORE|FULL)
           ;; # valid
      *)
          echo "ERROR: type_backup must be one of: CORE, FULL."
          ERROR=1
          ;;
  esac

  # CLEARUP: must be Y or N (any case)
  case "${CLEARUP^^}" in
      Y|N)
          ;; # valid
      *)
          echo "ERROR: clearup must be Y or N (upper or lowercase accepted)."
          ERROR=1
          ;;
  esac

  # Check Error Count
  if [[ "$ERROR" -ne 0 ]]; then
      echo "#------------------------------------------------"
      echo "# Validation failed. Please fix the errors above."
      echo "#------------------------------------------------"
      return 20
  else
      echo "#-----------------------------------------"
      echo "# Validation Successful. Script proceeding"
      echo "#-----------------------------------------"
      return 0
  fi

}

#
# Main Script Body
#

echo ""
echo "Make Postgres Backup Start : $(date '+%d/%m/%Y %H:%M:%S')"
echo "============================================================================================================="
echo ""

PROGNAME=$(basename $0)
PROGDIR=$(dirname $0)

# Define Variables
DATABASE=""
VERSION=""
USER=""
TYPE_BACKUP=""
CLEARUP=""


while getopts ":d:v:u:t:c:" opt; do
  case "$opt" in
    d) DATABASE="${OPTARG}" ;;
    v) VERSION="${OPTARG}" ;;
    u) USER="${OPTARG}" ;;
    t) TYPE_BACKUP="${OPTARG}" ;;	
    c) CLEARUP="${OPTARG}" ;;
    :) echo "Missing argument for -$OPTARG"
       usage
       exit 3 ;;
    ?) echo "Unknown option: -$OPTARG"
       usage
       exit 3 ;;
  esac
done

echo "DATABASE        = $DATABASE"
echo "VERSION         = $VERSION"
echo "USER            = $USER"
echo "TYPE_BACKUP     = $TYPE_BACKUP"
echo "CLEARUP         = $CLEARUP"

# Set the exitcode before process begins
EXITCODE=0

# Call parameter validation function
if ! validateInputParams
then
  EXITCODE=$?
  echo "$PROGNAME : ABORTING - Error in Input Parameter Validation"
  SYSTEM_TIME=$(date +"%d/%m/%Y %H:%M:%S")

  echo "Input Parameter Validation has failed on $DATABASE running as $USER at $SYSTEM_TIME - PLEASE INVESTIGATE"
  exit $EXITCODE
fi

# Generate File Location
BACKUP_FOLDER="/var/lib/pgsql/18/backups"
STAMP="$(date +%Y_%m_%d)"
OUTDIR="${BACKUP_FOLDER}/${DATABASE}_${STAMP}_${VERSION}"

mkdir "$OUTDIR"

chmod 777 "$OUTDIR"

# Run Database Snapshot ClearUp if Clearup variable = Y
if [[ ${CLEARUP^^} == "Y" ]]; then
    runDatabaseSnapshotClearUp
fi

# Check EXITCODE for failures
if [ $EXITCODE -ne 0 ]
then
  echo "$PROGNAME : ABORTING - Error in Snapshot Clearup Operation"
  SYSTEM_TIME=$(date +"%d/%m/%Y %H:%M:%S")

  echo "Snapshot Clearup Operation has failed on $DATABASE running as $USER at $SYSTEM_TIME - PLEASE INVESTIGATE"
  exit $EXITCODE
fi


# Determine which snapshot to create based on TYPE_BACKUP parameter
case "${TYPE_BACKUP^^}" in
    CORE)
        runDatabaseSnapshotCore
        ;;
    FULL)
        runDatabaseSnapshotFull
        ;;
esac

