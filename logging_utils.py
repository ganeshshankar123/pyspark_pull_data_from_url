def init_log(file_name):
    import logging
    logging.basicConfig(filename=file_name, format="%(asctime)s %(levelname)s : %(message)s", datefmt="%Y-%m-%d %h:%M:%S", level=logging.INFO)