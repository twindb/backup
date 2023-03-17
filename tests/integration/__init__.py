from os import environ

__author__ = "aleks"


def ensure_aws_creds():
    print("Integration tests need Amazon API credentials:")
    print(f"    AWS_ACCESS_KEY_ID     .. {'found' if 'AWS_ACCESS_KEY_ID' in environ else 'not found'}")
    print(f"    AWS_SECRET_ACCESS_KEY .. {'found' if 'AWS_SECRET_ACCESS_KEY' in environ else 'not found'}")
    try:
        try:
            environ["AWS_ACCESS_KEY_ID"]
        except KeyError:
            print("Environment variable AWS_ACCESS_KEY_ID is not defined.")
            environ["AWS_ACCESS_KEY_ID"] = input("Please enter it: ")
        try:
            environ["AWS_SECRET_ACCESS_KEY"]
        except KeyError:
            print("Environment variable AWS_SECRET_ACCESS_KEY is not defined.")
            environ["AWS_SECRET_ACCESS_KEY"] = input("Please enter it: ")
    except KeyboardInterrupt:
        exit(1)
