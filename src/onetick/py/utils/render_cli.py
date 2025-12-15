import argparse
from typing import Any, Dict, Optional

import onetick.py as otp


def parse_args():
    parser = argparse.ArgumentParser(description='Render queries from otq file')

    parser.add_argument('path', help='Path to otq file')
    parser.add_argument(
        '--image-path',
        help='Path to output image file. Default: query.svg in current working directory',
    )
    parser.add_argument('--output-format', help='Output format of output image')
    parser.add_argument(
        '--load-external-otqs', help='Load query dependencies from external otq files',
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        '--view', help='Show generated image after render', action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        '--y-limit', help='Limit for maximum number of lines of some EP parameters strings', type=int,
    )
    parser.add_argument(
        '--x-limit', help='Limit for maximum number of characters for each line in text of some EP parameters strings',
        type=int,
    )
    parser.add_argument(
        '--parse-eval-from-params', help='Enable parsing and printing `eval` sub-queries from EP parameters',
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        '--render-debug-info', help='Render additional debug information',
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        '--debug', help='Allow to print stdout or stderr from `Graphviz` render',
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        '--graphviz-compat-mode',
        help='Change internal parameters of result graph for better compatibility with old `Graphviz` versions',
        action=argparse.BooleanOptionalAction,
    )

    args = parser.parse_args()
    return args


def render_otq(
    path: str,
    image_path: Optional[str] = None,
    y_limit: Optional[int] = None,
    x_limit: Optional[int] = None,
    **kwargs,
):
    if image_path is None:
        if kwargs.get('output_format'):
            raise ValueError('`image-path` should be specified in order to use parameter `output-format`')

        image_path = 'query.svg'

    call_kwargs: Dict[str, Any] = {'path': path, 'image_path': image_path}
    if x_limit is not None and y_limit is not None:
        call_kwargs['line_limit'] = (y_limit, x_limit)
    elif x_limit is not None or y_limit is not None:
        raise ValueError('Both `y_limit` and `x_limit` should be set')

    for param_name in [
        'output_format', 'load_external_otqs', 'view', 'parse_eval_from_params', 'render_debug_info',
        'debug', 'graphviz_compat_mode',
    ]:
        if kwargs.get(param_name) is not None:
            call_kwargs[param_name] = kwargs[param_name]

    output_path = otp.utils.render_otq(**call_kwargs)
    print(f'Rendered graph saved in {output_path}')


def main():
    args = parse_args()
    render_otq(**vars(args))


if __name__ == '__main__':
    main()
