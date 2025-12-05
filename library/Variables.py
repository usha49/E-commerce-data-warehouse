# dynamically fetches database credentials from config.cfg

import json
class Variables:
    @staticmethod
    def get_value (var_name):
        config_path = "C:\\Users\\YOGA\\OneDrive\\Desktop\\last sem\\DataWarehouse\\DWBIlab\\config\\config.cfg"
        try:
            with open(config_path,"r", encoding="utf-8") as file:
                file_content=json.loads(file.read())
                if var_name in file_content:
                    return file_content[var_name]
                else:
                    return None

        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at {config_path}.")
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing Json in configuration file: {e}")
        except Exception as e:
            print (f"[Error in Variables]:{e}")

