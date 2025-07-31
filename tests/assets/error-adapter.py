#!/usr/bin/env python3

from daggerml.core import Error, to_json

if __name__ == "__main__":
    print(to_json(Error("Simulated adapter error.", type="error-adapter", origin="test")))
