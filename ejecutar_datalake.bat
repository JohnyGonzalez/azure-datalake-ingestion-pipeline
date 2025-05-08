@echo off
set FECHA=%DATE:/=-%_%TIME::=-%
set LOGFILE=log_datalake_%FECHA%.txt

echo Iniciando proceso de carga de datos... > %LOGFILE%
python script_proceso.py >> %LOGFILE% 2>&1
echo Proceso finalizado. >> %LOGFILE%
