import dask
import dask.dataframe as dd
import pandas as pd
import s3fs
import json
import dask.bag as db
import ast
import logging
import os
import datetime

import logging_utils as logUtils

def get_current_datetime():
    return str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))

def read_csv_from_url(data_url, data_file_cols=None):
    rating_movie_tv = dd.read_csv(data_url)
    rating_movie_tv.columns = data_file_cols
    return rating_movie_tv

def read_json_from_url(meta_url):
    meta_movie_tv = db.read_text(meta_url).map(ast.literal_eval)
    return meta_movie_tv

def write_datafile_to_S3(ddf, path, st_opt):
    ddf.to_csv(path, storage_options=st_opt)
    
def write_metadatafile_to_S3(dbag, path, st_opt):
    dbag.map(json.dumps).to_textfiles(path, storage_options=st_opt)

if __name__ == "__main__":
    logger = logging.getLogger("ingestion")
    err_code = 0
    app_name = "Data_Loading_toS3"
    
    code_home = os.getcwd()
    log_home = os.path.join(code_home, "log")
    
    if not os.path.exists(log_home):
        os.mkdir(log_home)
    
    log_name = "{0}_{1}.log".format(get_current_datetime(), app_name)
    
    log_path = os.path.join(log_home, log_name)
    logUtils.init_log(log_path)
    
    try:
        logger.info("-----------------------------------Script Commencement------------------------------------------")
        logger.info("Arguements Passed : {0}".format(args))
        
        config_file_path = os.path.join(code_home, "config.json")
        f = open(config_file_path)
        config = json.load(f)
        
        logger.info("Reading Data file from URL")
        rating_mv_tv_data = read_csv_from_url(config["DataFileUrl"], config["DataFileColumns"])
        
        logger.info("Writing Data file to S3")
        str_options = {"anon" : False, "key" : config["S3AccessKey"], "secret" : config["S3SecretKey"]}
        s3_file_path = "/".join["s3:/", config["S3BucketName"], config["S3DataFileName"]]
        logger.info("s3_file_path : ", s3_file_path)
        write_datafile_to_S3(rating_mv_tv_data, s3_file_path, str_options)
        
        logger.info("Reading MetaData file from URL")
        rating_mv_tv_meta = read_json_from_url(config["MetadataFileUrl"])
        
        logger.info("Writing MetaData file to S3")
        s3_meta_file_path = "/".join["s3:/", config["S3BucketName"], config["S3MetadataFileName"]]
        logger.info("s3_meta_file_path : ", s3_meta_file_path)
        write_metadatafile_to_S3(rating_mv_tv_meta, s3_meta_file_path, str_options)
        
        
    except Exception as e:
        logger.error(e)
        err_code = 1
        
    finally:
        logger.info("Progress exit with error code : {0}".format(err_code))
        exit(err_code)           
