import logging


class logger:
    def __init__(self):
        super().__init__()
        logging.basicConfig(level=logging.INFO)

    def getLogger(self, class_name):
        return logging.getLogger(class_name)
