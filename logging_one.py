import os
import json
import logging
import logging.config

#https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/
#https://docs.python.org/3/howto/logging.html

def setup_logging(

    default_path='/Users/kim/PycharmProjects/MDMImport_Modules/logging.json',
    default_level=logging.info,
):
    """Setup logging configuration

    """
    path = default_path
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
