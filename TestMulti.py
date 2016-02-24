#!/usr/bin/env python

import multiprocessing
import os

if __name__ == "__main__":
    print multiprocessing.cpu_count()
    print os.environ['PATH']
    with open('/testdir/message.txt','w') as f:
        f.write("This is awesome")
