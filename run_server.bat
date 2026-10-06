@echo off
REM Lanceur du serveur MCP Ableton, cote Windows.
REM Appele par Claude Code (WSL) via l'interop : les variables d'environnement
REM ne franchissent pas la frontiere WSL->Windows, elles doivent etre posees ici.
REM -X utf8 est obligatoire : sans lui, tout nom de piste ou de clip accentue
REM fait planter le serveur en UnicodeEncodeError (console Windows en cp1252).
chcp 65001 > NUL 2>&1
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set PYTHONUNBUFFERED=1
REM %~dp0 = dossier de ce .bat : aucun chemin propre a une machine.
cd /d "%~dp0"
"%~dp0.venv\Scripts\python.exe" -X utf8 -m MCP_Server.server
