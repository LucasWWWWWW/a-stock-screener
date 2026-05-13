"""把当前 web/data/stocks.json 精简存为 web/data/stocks.prev.json (用于今日 diff)"""

import json
from pathlib import Path

src_path = Path("web/data/stocks.json")
if not src_path.exists():
    print("no stocks.json yet, skipping snapshot")
    raise SystemExit(0)

src = json.loads(src_path.read_text(encoding="utf-8"))
prev = {
    "generated_at": src.get("generated_at"),
    "trade_date": src.get("trade_date"),
    "codes": [s["code"] for s in src.get("stocks", [])],
    "codes_meta": {
        s["code"]: {"name": s["name"], "industry": s.get("industry", "")}
        for s in src.get("stocks", [])
    },
}
out_path = Path("web/data/stocks.prev.json")
out_path.write_text(json.dumps(prev, ensure_ascii=False), encoding="utf-8")
print(f"snapshot saved: {len(prev['codes'])} codes")
