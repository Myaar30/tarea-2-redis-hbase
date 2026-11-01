$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
if (-not (Test-Path -LiteralPath '.venv\Scripts\python.exe')) {
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo crear el ambiente virtual' }
}
& '.\.venv\Scripts\python.exe' -m pip install --disable-pip-version-check -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw 'No se pudieron instalar las dependencias' }
docker compose up -d --build --wait
if ($LASTEXITCODE -ne 0) { throw 'No se pudieron iniciar los servicios' }
& '.\.venv\Scripts\python.exe' -m tarea.run @args
if ($LASTEXITCODE -ne 0) { throw 'La ejecucion no termino correctamente; revisar resultados y mensajes' }
