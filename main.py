import logging
import sys
import requests

def main():
    logging.info("Strat")
    response = requests.get("https://www.googleapis.com/youtube/v3/playlists", params ={})
    logging.debug("GOT %s", response.text)


if __name__  == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    sys.exit(main())
