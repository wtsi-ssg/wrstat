#!/bin/bash

set -euo pipefail;
IFS=$'\n\t';

declare DATE="$(date "+%Y%m%d")";
declare -a TIDY_FLAGS=();
declare -a BASEDIR_FLAGS=();
declare OUTPUT="";
declare FINAL="";
declare WRSTAT="wrstat";
declare SET=(false false false false false false)

help() {
	echo "Usage: $0 [-b BASEDIRS_CONFIG] -f FINAL_OUTDIR -o OWNERS_FILE -q QUOTA_FILE [-w WRSTAT_EXE] WORKING_DIR";
	echo;
	echo "This script is used to run the final two steps of a full WRStat run, the 'basedirs' and 'tidy' steps.";
	echo;
	echo "	-b,--config	path to basedirs config file.";
	echo "	-d,--date	date for output files. [default: $DATE]";
	echo "	-f,--final	final output directory.";
	echo "	-h,--help	print this help text.";
	echo "	-o,--owners	gid,owner csv file.";
	echo "	-q,--quota	csv of gid,disk,size_quota,inode_quota.";
	echo "	-w,--wrstat	location of wrstat executable. [default: $WRSTAT]";
}

error() {
	echo -e "Error: $1.\n" >&2;
	help >&2;

	exit 1;
}

set() {
	if "${SET[$1]}"; then
		error "Can only set flag $2 once.";
	fi;

	if [ -z "$3" ]; then
		error "Value of $2 cannot be empty";
	fi;

	SET[$1]=true;
}

while [ $# -gt 0 ]; do
	case "$1" in
	"-q"|"--quota")
		set 0 "$1" "${2:-}";

		shift;

		BASEDIR_FLAGS+=( "-q" "$1" );;
	"-o"|"--owners")
		set 1 "$1" "${2:-}";

		shift;

		BASEDIR_FLAGS+=( "-o" "$1" );;
	"-f"|"--final")
		set 2 "$1" "${2:-}";

		shift;

		FINAL="$1";

		TIDY_FLAGS+=( "-f" "$1" );;
	"-b"|"--config")
		set 3 "$1" "${2:-}";

		shift;

		BASEDIR_FLAGS+=( "-b" "$1" );;
	"-d"|"--date")
		set 4 "$1" "${2:-}";

		shift;

		DATE="$1";;
	"-w"|"--wrstat")
		set 5 "$1" "${2:-}";

		shift;

		WRSTAT="$1";;
	"-h"|"--help")
		help;

		exit 0;;
	*)
		if [ -s "$OUTPUT" ]; then
			error "Can only set a single output directory."
		fi;

		OUTPUT="$1";;
	esac;

	shift;
done;

declare -i flag=0;

for var in "Quota CSV" "Owners CSV" "Final Output Directory"; do
	if [ "${SET[$flag]}" = "false" ]; then
		error "$var is required.";
	fi;

	flag=$(( $flag + 1 ));
done;

if [ -z "$OUTPUT" ]; then
	error "No Working Directory specified.";
fi;

TIDY_FLAGS+=( "-d" "$DATE" "$OUTPUT" );
BASEDIR_FLAGS+=( "$OUTPUT" "$FINAL" );

"$WRSTAT" basedir "${BASEDIR_FLAGS[@]}";
"$WRSTAT" tidy "${TIDY_FLAGS[@]}";
