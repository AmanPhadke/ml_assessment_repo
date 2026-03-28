#importing necessary libraries
import argparse
import os
import yaml
import pandas as pd
import logging
import time
import json
import numpy as np


#main function
def main():
    #calculating start time of the file for calculating latency in the later functions
    start_time = time.time()

    #see if the file exists or not
    def valid_file(path):
        if not os.path.exists(path):
            raise argparse.ArgumentTypeError(f"{path} does not exist, Please enter a valid path")
        return path

    #parsing the given arguments to extract each file from the CLA
    def arguments():
        parser = argparse.ArgumentParser(description='Files Input')

        #extracting input file
        parser.add_argument('--input',type= valid_file ,help = 'Add input file path', required = True)
        #extracting config file
        parser.add_argument('--config', type= valid_file ,help = 'Add configuration file path', required = True)
        #extracting output file
        parser.add_argument('--output',help = 'Add output file path', required = True)
        #extracting log file
        parser.add_argument('--log-file',help = 'Add log file path', required = True)

        return parser.parse_args()

    #checking config file's validity
    def yaml_file_validity(config_file):

        with open(config_file, "r") as file:
            config = yaml.safe_load(file)

            #checking for missing values
            if ('seed') not in config:
                raise ValueError("Missing required config key: 'seed'")
            if ('window') not in config:
                raise ValueError("Missing required config key: 'window'")
            if ('version') not in config:
                raise ValueError("Missing required config key: 'version'")
            else:
                if config['seed'] is None:
                    raise ValueError("Config key 'seed' is empty")
                if config['window'] is None:
                    raise ValueError("Config key 'window' is empty")
                if config['version'] is None:
                    raise ValueError("Config key 'version' is empty")
                
                return config

    #verifying csv file's validity and error handeling
    def csv_validity(csv_path):
        df = pd.read_csv(csv_path)
        
        if ('close') not in df.columns:
            raise ValueError(f'Missing column in {csv_path}: "close"')
        
        is_empty = df['close'].empty
        if is_empty:
            raise ValueError(f'"close" column in {csv_path} is empty')

        return df

    #creating the log infos to display in the command line
    def log_file_creation(log_file):
        logger = logging.getLogger(__name__)
        #checking on each level
        logger.setLevel(logging.INFO)

        #date reformatting
        formatter = logging.Formatter(
            fmt='[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger


    #calculating the rolling mean with the help of extracted window from config
    def rolling_mean(df, window):
        df['rolling_mean'] = round(df['close'].rolling(window = window).mean(),2)
        return df

    #signal detection using lambda function
    def signal_detection(rm_df):
        rm_df['signal'] = rm_df.apply(
            lambda row: 1 if row['close'] > row['rolling_mean'] else 0, axis = 1
        )

        return rm_df

    #calculating metrics to display and to store in metrics.json
    def metrics_calculation(df, seed, version, start_time):
        metrics = {}

        rows_processed = df.shape[0]

        signal_rate = df['signal'].mean()

        #calculating time when almost all processes are executed
        end_time = time.time()
        #calculating the amount of time the program took to run
        latency = int((end_time - start_time) * 1000)


        metrics['version'] = version
        metrics['rows_processed'] = rows_processed
        metrics['metric'] = 'signal_rate'
        metrics['value'] = float(signal_rate)
        metrics['latency_ms'] = latency
        metrics['seed'] = seed
        metrics['status'] = 'success'

        return metrics

    #creating the metrics.json file and returning it as stdout for the terminal display
    def output_file_creation(output_path, metrics):
        with open(output_path, "w") as f:
            json.dump(metrics, f, indent=4)

        
        json_print = json.dumps(metrics, indent=4)
        return json_print


    #starting the job
    args = arguments()
    logs = log_file_creation(args.log_file)
    logs.info("Job started")

    try: 
        logs.info("Config loaded successfully")
        config = yaml_file_validity(args.config)
        seed, window, version = config['seed'], config['window'], config['version']
        logs.info(f"Seed: {seed}, Window: {window}, Version: {version}")
        np.random.seed(seed) #to get the same result with the same seed everytime
        df = csv_validity(args.input)
        logs.info(f"Data loaded: {len(df)} rows")
        rolling_mean_df = rolling_mean(df, window)
        logs.info("Rolling mean computed")
        signal_df = signal_detection(rolling_mean_df)
        logs.info("Signals generated")
        metrics = metrics_calculation(signal_df, seed, version, start_time)
        logs.info("Metrics calculated")
        json_print = output_file_creation(args.output, metrics)
        logs.info("Job completed successfully")
        print(json_print)

        return 0
    
    #handeling exceptions and creating a seperate json for it
    except Exception as e:
        #finding out the error cause
        logs.error(f"Job failed: {str(e)}")
    
        error_metrics = {
            "version": version if 'version' in locals() else "unknown",
            "status": "error",
            "error_message": str(e)
        }
        try:
            with open(args.output, 'w') as f:
                json.dump(error_metrics, f, indent=4)

            print(json.dumps(error_metrics, indent=4))

        except:
            pass
        
        return 1

if __name__ == "__main__":
    main()
