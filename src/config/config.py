import yaml

class Config():
    def __init__(self,file_name) -> None:
        with open(f'./config/{file_name}.yaml',encoding='utf-8') as yaml_file:
            self.config = yaml.load(yaml_file, Loader=yaml.FullLoader)

