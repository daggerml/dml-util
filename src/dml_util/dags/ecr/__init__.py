#!/usr/bin/env python3
import json
from pathlib import Path

_here_ = Path(__file__).parent


def load():
    with open(_here_ / "cf.json") as f:
        js = json.load(f)
    return js, {}, "Uri", None


if __name__ == "__main__":
    print(json.dumps(list(load())))
