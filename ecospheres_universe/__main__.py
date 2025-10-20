import sys
from minicli import run

from ecospheres_universe.feed_universe import feed_universe
from ecospheres_universe.check_sync import check_sync


def main():
    sys.argv[0] = "ecospheres-universe"
    run()


if __name__ == "__main__":
    main()
