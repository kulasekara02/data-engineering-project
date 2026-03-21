"""Server entry point."""
import sys
import os
import uvicorn

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)

from src.api.main import app  # noqa

if __name__ == '__main__':
    routes = [r.path for r in app.routes if hasattr(r, 'path')]
    print(f"Starting server with {len(routes)} routes")
    for r in routes:
        if 'live' in r or 'advanced' in r:
            print(f"  {r}")
    uvicorn.run(app, host='0.0.0.0', port=8000)
