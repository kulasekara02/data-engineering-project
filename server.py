"""Server entry point - imports app and runs it."""
import sys
import os

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

# Force clean module cache
for key in list(sys.modules.keys()):
    if key.startswith('src'):
        del sys.modules[key]

from src.api.main import app  # noqa

if __name__ == '__main__':
    import uvicorn
    print(f"Registered {len([r for r in app.routes if hasattr(r, 'path')])} routes")
    uvicorn.run("server:app", host='0.0.0.0', port=8000, reload=True, reload_dirs=[BASE])
