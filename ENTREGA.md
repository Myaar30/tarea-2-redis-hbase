# Archivos para entregar

**Integrantes:** Nicolas Galaz, Martin Yañez y Eduardo Roa.

El informe es `informe.pdf`. Tiene las respuestas de P1 a P6, las pruebas D1 y D2, la tabla de motores, las mediciones y las conclusiones. Los archivos `resultados/*.json` permiten revisar las cifras y los tiempos originales.

El resto de esta carpeta es el proyecto que debe publicarse como repositorio Git cuando decidamos subirlo. Las instrucciones están en `README.md`. Para reproducir toda la tarea en Windows se ejecuta `powershell -ExecutionPolicy Bypass -File .\run.ps1`; en Linux o macOS, `bash run.sh`.

Antes de enviar por Aula:

1. Revisar el PDF y los nombres de los integrantes.
2. Publicar el proyecto como repositorio **público**, incluyendo el informe y los resultados agregados. La carpeta ya tiene Git inicializado.
3. Abrir el enlace sin iniciar sesión para comprobar que está accesible.
4. Entregar en Aula el enlace del repositorio y el PDF del informe.

El enunciado fija la entrega para el domingo 8 de noviembre de 2026 a las 23:59. No se deben subir `.venv/`, `.runtime/`, archivos de datos generados ni volúmenes de Docker; ya están excluidos por `.gitignore`.

Los servicios conservan la historia en volúmenes del proyecto. Se pueden apagar con `docker compose down`. Volver a ejecutar el script completo reconstruye la entrega desde cero y reemplaza los resultados anteriores.
