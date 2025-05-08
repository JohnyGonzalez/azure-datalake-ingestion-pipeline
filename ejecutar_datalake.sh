#!/bin/bash

FECHA=$(date +"%Y-%m-%d_%H-%M-%S")
LOGFILE="log_datalake_$FECHA.log"

echo "Iniciando proceso de carga de datos..." > "$LOGFILE"
python3 script_proceso.py >> "$LOGFILE" 2>&1
echo "Proceso finalizado." >> "$LOGFILE"
