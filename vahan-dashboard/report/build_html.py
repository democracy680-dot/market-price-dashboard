import os
import tempfile

from report import template


def write_dashboard(view, out_path: str) -> None:
    if not view.get("categories"):
        raise ValueError("Refusing to write dashboard: view has no categories (empty/failed run).")
    html = template.render(view)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    # Write to a temp file then atomically replace, so a good file is never left half-written.
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(out_path)), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(html)
        os.replace(tmp, out_path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
