import sys
import os
from pathlib import Path
module_path = os.path.abspath(os.pardir)
if module_path not in sys.path:
    sys.path.append(module_path)

if __name__ == "__main__":
    pass