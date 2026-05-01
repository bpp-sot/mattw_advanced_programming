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

  # Validate directory
  if [[ ! -d "${BACKUP_FOLDER}" ]]; then
      echo "ERROR: Directory '${BACKUP_FOLDER}' does not exist."
      return 1
  else
      echo "Directory '${BACKUP_FOLDER}' exists"
  fi

  # Count total subdirectories in directory
  local file_count
  file_count=$(find "$BACKUP_FOLDER" -mindepth 1 -type d | wc -l)

  echo "Found $file_count backups in '${BACKUP_FOLDER}'."

  # Rule: If 3 or fewer files exist ’ do NOT delete anything
  if (( file_count <= 3 )); then
      echo "Only $file_count files present. No Clearup performed."
      echo "#-----------------------------"
      echo "# Snapshot Clearup Completed"
      echo "#-----------------------------"
      return 0   # Function exits at this point
  fi

  echo "Scanning for files older than $days_old days..."

  # Find files older than 10 days
  old_files=$(find "$BACKUP_FOLDER" -maxdepth 1 -mindepth 1 -type d -mtime +"$days_old" | wc -l)

  echo "Found $old_files backups in '${BACKUP_FOLDER}' older than $days_old."


  if [[ -z "$old_files" ]]; then
      echo "No files older than $days_old days found."
      echo "#-----------------------------"
      echo "# Snapshot Clearup Completed"
      echo "#-----------------------------"
      return 0	# Function exits at this point
  fi

  echo "Deleting files older than $days_old days:"
  
  # Directories in scope of deletion
  preview_backups=$(find "$BACKUP_FOLDER" -mindepth 1 -maxdepth 1 -type d -mtime +"$days_old")
  echo "Backups in scope of deletion $preview_backups."

  # Delete them	& log what's being removed
  find "$BACKUP_FOLDER" -mindepth 1 -maxdepth 1 -type d -mtime +"$days_old" \
    -exec sh -c 'echo "Deleting: $1"; rm -rf "$1"' _ {} \;

  echo "#-----------------------------"
  echo "# Snapshot Clearup Completed"
  echo "#-----------------------------"
  return 0
}

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

  chmod 777 "${OUTDIR}/${DATABASE}_${TYPE_BACKUP}_snapshot.sql"
  chmod 777 "${OUTDIR}/roles_globals.sql"

  echo "Backup created in: ${OUTDIR}"
  echo " - roles_globals.sql"
  echo " - ${DATABASE}_${TYPE_BACKUP}_snapshot.sql (combined)"

  return 0
}

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
      CORE|CORE_SELECT|FULL)
           ;; # valid
      *)
          echo "ERROR: type_backup must be one of: CORE, CORE_SELECT, FULL."
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

#mkdir "$OUTDIR"

#chmod 777 "$OUTDIR"

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
    CORE_SELECT)
        echo "CORE_SELECT Chosen"
        ;;
    FULL)
        runDatabaseSnapshotFull
        ;;
esac

