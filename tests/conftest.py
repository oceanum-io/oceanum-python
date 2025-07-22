import os

test_token = os.getenv('OCEANUM_TEST_TOKEN', None)

if not test_token:
    raise ValueError(
        "Environment variable 'OCEANUM_TEST_TOKEN' is not set. "
        "Please set it to a valid token for testing."
    )

os.environ['DATAMESH_TOKEN'] = test_token


