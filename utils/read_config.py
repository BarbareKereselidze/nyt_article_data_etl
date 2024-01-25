import configparser


def read_config_file(config_file_path: str) -> dict:
    config_object = configparser.ConfigParser()

    with open(config_file_path, "r") as file:
        config_object.read_file(file)

    output_dict = {section: dict(config_object.items(section)) for section in config_object.sections()}

    return output_dict
