import re
from typing import Dict, Optional

RX = {
    "title": re.compile(r"^Title:\s*(.*)$", re.IGNORECASE | re.MULTILINE),
    "author": re.compile(r"^Author:\s*(.*)$", re.IGNORECASE | re.MULTILINE),
    "language": re.compile(r"^Language:\s*(.*)$", re.IGNORECASE | re.MULTILINE),
    "release_date": re.compile(r"^Release Date:\s*(.*)$", re.IGNORECASE | re.MULTILINE),
}

def parse_header(header_text: str) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    for k, rx in RX.items():
        m = rx.search(header_text)
        out[k] = m.group(1).strip() if m else None
    return out
