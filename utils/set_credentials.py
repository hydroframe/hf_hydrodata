"""Set user credentials for hf_hydrodata package."""
# pylint: disable=C0413

import sys
import os
import argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
import hf_hydrodata as hf

def get_arguments():
    """Get command-line arguments."""
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--public', action='store_true')
    group.add_argument('--private', action='store_true')

    return parser.parse_args()

def main():
    """Use environment variable secrets to set hf_hydrodata credentials
       for GitHub Actions testing."""
    args = get_arguments()
    if args.private:
        test_email = os.environ['TEST_EMAIL_PRIVATE']
        test_pin = os.environ['TEST_PIN_PRIVATE']
    else:
        test_email = os.environ['TEST_EMAIL_PUBLIC']
        test_pin = os.environ['TEST_PIN_PUBLIC']

    hf.register_api_pin(test_email, test_pin)

if __name__ == '__main__':
    main()
