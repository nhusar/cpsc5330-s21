#!/usr/bin/env python3

import re
import os
import sys


_RE_WORD = re.compile(r"[a-z]+")
_DOC_ID = os.path.splitext(os.path.basename(os.getenv('map_input_file')))[0]

for ln in sys.stdin:
    for term in _RE_WORD.findall(ln.strip().lower()):
        print("%s\t%s" % (term, _DOC_ID))
