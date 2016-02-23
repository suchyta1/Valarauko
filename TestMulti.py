#!/usr/bin/env python

import multiprocessing

if __name__ == "__main__":
    print multiprocessing.cpu_count()
    with open('/testdir/message.txt','w') as f:
        f.write("This is awesome")
