import yaml


class ConfigLoader:
    @staticmethod
    def load_config(config_path='src/config/config.yml'):
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
