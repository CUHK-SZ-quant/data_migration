import logging
import argparse
import yaml
import copy
import sys, os

#  sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class Config_Parser(argparse.ArgumentParser):
    """
    This class is used to load or save the arguments from or to a yaml config file
    """
    def __init__(self, *args, **kwargs):
        self.config_parser = argparse.ArgumentParser(add_help=False)
        self.config_parser.add_argument(
           '-c', '--config', default = None, metavar = 'FILE', help = 'where to load YAML configuration'
        )
        self.options = []
        super().__init__(*args, parents=[self.config_parser], formatter_class = argparse.RawDescriptionHelpFormatter, **kwargs)

    def add_argument(self, *args, **kwargs):
        arg = super().add_argument(*args, **kwargs)
        self.options.append(arg.dest)
        return arg

    def parse_args(self, args=None):
        arg, remaining_argv = self.config_parser.parse_known_args()

        if arg.config is not None:
            with open(arg.config, 'r') as f:
                config_args = yaml.safe_load(f)
            for key in config_args:
                if key not in self.options:
                    self.error(f'Unknown configuration entry: {key}')
            self.set_defaults(**config_args)

        return super().parse_args(remaining_argv)

    def save_args(self, args:argparse.Namespace, file_dir:str, exclude:list=None) -> None:
        assert isinstance(args, argparse.Namespace), TypeError(f'The args variable should be an argparse.Namespace type, instead of {type(args)}')
        assert os.path.splitext(file_dir)[-1] == '.yaml', TypeError(f'The save file should be a yaml config file, instead of {os.path.splitext(file_dir)}')

        if os.path.isfile(file_dir):
            #  raise Exception(f'The file {file_dir} has existed')
            os.remove(file_dir)
        if not os.path.isdir(os.path.dirname(os.path.abspath(file_dir))):
            os.makedirs(os.path.dirname(os.path.abspath(file_dir)))
        result_dict = copy.deepcopy(args.__dict__)
        for field in exclude or ['config']:
            result_dict.pop(field)
        with open(file_dir, 'w') as fp:
            yaml.dump(result_dict, fp)
        print(f'Args have been saved to {file_dir}')