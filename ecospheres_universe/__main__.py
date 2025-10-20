import sys
from minicli import run

from ecospheres_universe.feed_universe import feed_universe
from ecospheres_universe.check_sync import check_sync

if __name__ == "__main__":
    sys.argv[0] = "ecospheres_universe"
    run()
