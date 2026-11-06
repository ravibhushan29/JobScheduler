"""Write the API's OpenAPI document to sdk/openapi.json (used to generate SDK types).

python scripts/export_openapi.py            # write it
python scripts/export_openapi.py --check    # exit 1 if the committed file is out of date (CI)
"""

import json
import os
import sys
import tempfile
from pathlib import Path

# A throwaway database and the in-memory broker: importing the app must not touch real services.
_tmp = tempfile.mkdtemp()
os.environ.setdefault("DATABASE_URL", f"sqlite:///{_tmp}/openapi.db")
os.environ.setdefault("CELERY_BROKER_URL", "memory://")
os.environ.setdefault("CELERY_RESULTS_DB_PATH", f"{_tmp}/results.db")
os.environ.setdefault("LOG_TO_FILE", "false")
os.environ.setdefault("LOG_LEVEL", "ERROR")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import app  # noqa: E402

target = Path(__file__).resolve().parents[1] / "sdk" / "openapi.json"
current = json.dumps(app.openapi(), indent=2) + "\n"
if "--check" in sys.argv:
    if not target.exists() or target.read_text() != current:
        print(
            f"{target} is out of date: run `python scripts/export_openapi.py` and `npm run generate` in sdk/typescript"
        )
        sys.exit(1)
    print("sdk/openapi.json is up to date")
else:
    target.write_text(current)
    print(f"wrote {target} ({len(app.openapi()['paths'])} paths)")
