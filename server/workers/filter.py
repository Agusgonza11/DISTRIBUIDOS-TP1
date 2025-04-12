import logging
from common.utils import initialize_log

def main():
    initialize_log("INFO")
    logging.info("Se inicializó el worker filter")
    return

if __name__ == "__main__":
    main()
