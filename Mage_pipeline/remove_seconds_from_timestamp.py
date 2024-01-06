import datetime
import pandas as pd
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(all_data, *args, **kwargs):
    """
    Args:
        data: The output from the upstream parent block (if applicable)
        args: The output from any additional upstream blocks

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Round timestamp
    all_data['timestamp'] = pd.to_datetime(all_data['timestamp'])
    all_data['timestamp'] = all_data['timestamp'].apply(lambda x: x.replace(second=0))

    #convert it back to string format without seconds
    all_data['timestamp'] = all_data['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
    df = all_data

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
