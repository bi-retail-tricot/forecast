import sys
from pathlib import Path

# Obtener ruta del notebook (donde estás parado)
NOTEBOOK_DIR = Path().resolve()
ROOT_DIR = NOTEBOOK_DIR.parent

# Añadir al sys.path si no está
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

print(f"Now you can import modules from the project root: {ROOT_DIR}")