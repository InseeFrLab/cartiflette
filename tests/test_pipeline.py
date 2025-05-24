

from cartiflette.pipeline import restructure_nested_dict_borders

def test_restructure_nested_dict_borders():
    sample_dict = {'a': [1, 2, 3], 'b': [4, 5]}
    expected_result = [['a', 1], ['a', 2], ['a', 3], ['b', 4], ['b', 5]]
    assert restructure_nested_dict_borders(sample_dict) == expected_result

    empty_dict = {}
    assert restructure_nested_dict_borders(empty_dict) == []

    single_item_dict = {'a': [1]}
    assert restructure_nested_dict_borders(single_item_dict) == [['a', 1]]
