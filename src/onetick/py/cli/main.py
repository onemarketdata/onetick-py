"""
Adds support for custom commands to CLI `onetick` command.

In order to add your custom command create module in your package which implements two functions:
- `run(args: argparse.Namespace)` - code to run on command call
- `parser_impl(parser: argparse.ArgumentParser)` - adding arguments to passed `ArgumentParser` object

And add your command to `onetick.py.cli.plugins` in `entry_points` section in `setup.py`:
```
    entry_points={
        'onetick.py.cli.plugins': [
            'your_command = module.with.command.code'
        ]
    }
```

You can see example of code implementation in `onetick.py.cli.render` and
definition of command in `entry_points` in `setup.py`.
"""

import sys
import argparse
import warnings
from typing import Callable, Dict, Optional
from dataclasses import dataclass
from importlib import metadata

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


@dataclass
class Command:
    name: str
    module_name: str
    func: Callable
    args_parser: Optional[Callable]


def compatibility_check():
    onetick_cli_packages = [
        dist.name for dist in metadata.distributions()
        if dist.name.startswith('onetick-ext') or dist.name.startswith('onetick-cli')
    ]
    if 'onetick-cli' not in onetick_cli_packages:
        return

    ext_packages_str = ', '.join(ext_pkg for ext_pkg in onetick_cli_packages if ext_pkg != 'onetick-cli')
    warnings.warn(
        f'You are using the next outdated packages: {ext_packages_str}. '
        'They rely on `onetick-cli` package, which conflicts with current version of `onetick-py`. '
        'You should either install latest versions of these packages, '
        'or install `onetick-py<1.188.0` instead of currently installed version.'
    )


def load_commands() -> dict:
    commands: Dict[str, Command] = {}
    available_commands = entry_points(group='onetick.py.cli.plugins')
    for command_loader in available_commands:
        command = command_loader.name
        module_name = command_loader.value
        command_module = command_loader.load()
        run_impl = getattr(command_module, 'run', None)
        parser_impl = getattr(command_module, 'parser_impl', None)

        if run_impl is None:
            warnings.warn(f"Plugin {module_name} misconfigured: function `run` not found.")
            continue

        if command in commands:
            warnings.warn(
                f"Command `{command}` already declared by plugin `{commands[command].module_name}`. "
                f"It won't be loaded from plugin `{module_name}`."
            )

        commands[command] = Command(name=command, module_name=module_name, func=run_impl, args_parser=parser_impl)

    return commands


def load_arg_parsers(commands: Dict[str, Command], parser: argparse.ArgumentParser):
    subparsers = parser.add_subparsers()

    for command, command_obj in commands.items():
        sub_parser = subparsers.add_parser(command)

        if command_obj.args_parser is not None:
            command_obj.args_parser(sub_parser)

        sub_parser.set_defaults(func=command_obj.func)


def otp_cli():
    compatibility_check()

    commands = load_commands()

    if len(commands):
        epilog = 'List of available subcommands : %s' % ', '.join(commands)
    else:
        epilog = 'There is no available subcommands, because no compatible plugins are installed.'

    parser = argparse.ArgumentParser(prog='onetick', epilog=epilog)

    group = parser.add_mutually_exclusive_group()

    group.add_argument('--version', action='store_true',
                       help="show the onetick command's version and exit")

    if len(commands):
        load_arg_parsers(commands, parser)

    args = parser.parse_args()

    if 'func' in args:
        args.func(args)
    else:
        parser.print_help()
