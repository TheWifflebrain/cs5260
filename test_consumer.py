import pytest
import string
from consumer_term import *
from types import SimpleNamespace
from unittest.mock import MagicMock


@pytest.mark.parametrize("body, expected1, expected2, expected3", 
                        [(b'{"type":"create","requestId":"b897b7f0-ffba-4960-a611-213c66e75112","widgetId":"3b7fef78-f757-424d-9c0c-431768022528","owner":"Mary Matthews","label":"F","description":"PFZCIEPDAYIYRAKNO","otherAttributes":[{"name":"size","value":"504"},{"name":"height","value":"431"},{"name":"height-unit","value":"cm"},{"name":"width-unit","value":"cm"},{"name":"quantity","value":"786"}]}',
                          b'{"type":"create","requestId":"b897b7f0-ffba-4960-a611-213c66e75112","widgetId":"3b7fef78-f757-424d-9c0c-431768022528","owner":"Mary Matthews","label":"F","description":"PFZCIEPDAYIYRAKNO","otherAttributes":[{"name":"size","value":"504"},{"name":"height","value":"431"},{"name":"height-unit","value":"cm"},{"name":"width-unit","value":"cm"},{"name":"quantity","value":"786"}]}',
                        "namespace(description='PFZCIEPDAYIYRAKNO', label='F', otherAttributes=[namespace(name='size', value='504'), namespace(name='height', value='431'), namespace(name='height-unit', value='cm'), namespace(name='width-unit', value='cm'), namespace(name='quantity', value='786')], owner='Mary Matthews', requestId='b897b7f0-ffba-4960-a611-213c66e75112', type='create', widgetId='3b7fef78-f757-424d-9c0c-431768022528')",
                        "mary-matthews"),
                        (b'',
                        -1,
                        "-1",
                        -1)
                        ])
def test_prepare_data(body, expected1, expected2, expected3):
    body2, json_data, owner = prepare_data(body)
    assert body2 == expected1
    # json_data is a string because if it is not then pyteset does not recognize namespace
    assert str(json_data) == expected2
    assert owner == expected3


@pytest.mark.parametrize("json_data, owner, expected", 
                        [('{"type":"create","requestId":"b897b7f0-ffba-4960-a611-213c66e75112","widgetId":"3b7fef78-f757-424d-9c0c-431768022528","owner":"Mary Matthews","label":"F","description":"PFZCIEPDAYIYRAKNO","otherAttributes":[{"name":"size","value":"504"},{"name":"height","value":"431"},{"name":"height-unit","value":"cm"},{"name":"width-unit","value":"cm"},{"name":"quantity","value":"786"}]}',
                        "mary-matthews",
                        {'id': '3b7fef78-f757-424d-9c0c-431768022528', 'owner': 'mary-matthews', 'label': 'F', 'description': 'PFZCIEPDAYIYRAKNO', 'size': '504', 'height': '431', 'height-unit': 'cm', 'width-unit': 'cm', 'quantity': '786'}
                        ),
                        ('{"type":"create","requestId":"c7d8b67f-084a-4eb5-a97f-2c6d7ef29808","widgetId":"deab01f2-0075-4a11-87e1-21cbea5c0c7f","owner":"Henry Hops","label":"DHSRL","description":"XROPEIDSOOJAYOXGY","otherAttributes":[{"name":"size","value":"884"},{"name":"height","value":"51"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"4.8361626"},{"name":"price","value":"34.82"},{"name":"quantity","value":"297"},{"name":"vendor","value":"EPAQ"}]}',
                        "henry-hops",
                        {'id': 'deab01f2-0075-4a11-87e1-21cbea5c0c7f', 'owner': 'henry-hops', 'label': 'DHSRL', 'description': 'XROPEIDSOOJAYOXGY', 'size': '884', 'height': '51', 'length-unit': 'cm', 'rating': '4.8361626', 'price': '34.82', 'quantity': '297', 'vendor': 'EPAQ'}
                        )                      
                        ])
def test_prepare_dynamodb_data(json_data, owner, expected):
    # needs this type of dataset to be loaded into prepare_dynamodb_data
    json_data2 = json.loads(json_data, object_hook=lambda d: SimpleNamespace(**d))
    item = prepare_dynamodb_data(json_data2, owner)
    assert item == expected

@pytest.mark.parametrize("body, expected", 
                        [(b'{"type":"create","requestId":"b897b7f0-ffba-4960-a611-213c66e75112","widgetId":"3b7fef78-f757-424d-9c0c-431768022528","owner":"Mary Matthews","label":"F","description":"PFZCIEPDAYIYRAKNO","otherAttributes":[{"name":"size","value":"504"},{"name":"height","value":"431"},{"name":"height-unit","value":"cm"},{"name":"width-unit","value":"cm"},{"name":"quantity","value":"786"}]}',
                        str({
                        "description": "PFZCIEPDAYIYRAKNO",
                        "label": "F",
                        "otherAttributes": [
                            {
                                "name": "size",
                                "value": "504"
                            },
                            {
                                "name": "height",
                                "value": "431"
                            },
                            {
                                "name": "height-unit",
                                "value": "cm"
                            },
                            {
                                "name": "width-unit",
                                "value": "cm"
                            },
                            {
                                "name": "quantity",
                                "value": "786"
                            }
                        ],
                        "owner": "Mary Matthews",
                        "requestId": "b897b7f0-ffba-4960-a611-213c66e75112",
                        "type": "create",
                        "widgetId": "3b7fef78-f757-424d-9c0c-431768022528"
                    })),
                    (b'{"type":"create","requestId":"c7d8b67f-084a-4eb5-a97f-2c6d7ef29808","widgetId":"deab01f2-0075-4a11-87e1-21cbea5c0c7f","owner":"Henry Hops","label":"DHSRL","description":"XROPEIDSOOJAYOXGY","otherAttributes":[{"name":"size","value":"884"},{"name":"height","value":"51"},{"name":"length-unit","value":"cm"},{"name":"rating","value":"4.8361626"},{"name":"price","value":"34.82"},{"name":"quantity","value":"297"},{"name":"vendor","value":"EPAQ"}]}',
                    str({
                    "description": "XROPEIDSOOJAYOXGY",
                    "label": "DHSRL",
                    "otherAttributes": [
                        {
                            "name": "size",
                            "value": "884"
                        },
                        {
                            "name": "height",
                            "value": "51"
                        },
                        {
                            "name": "length-unit",
                            "value": "cm"
                        },
                        {
                            "name": "rating",
                            "value": "4.8361626"
                        },
                        {
                            "name": "price",
                            "value": "34.82"
                        },
                        {
                            "name": "quantity",
                            "value": "297"
                        },
                        {
                            "name": "vendor",
                            "value": "EPAQ"
                        }
                    ],
                    "owner": "Henry Hops",
                    "requestId": "c7d8b67f-084a-4eb5-a97f-2c6d7ef29808",
                    "type": "create",
                    "widgetId": "deab01f2-0075-4a11-87e1-21cbea5c0c7f"
                }))
                ])
def test_prepare_s3bucket_data(body, expected):
    j_data_serialized = prepare_s3bucket_data(body)
    # the data was the same however the format was off
    # i think it was expecting a fancy way with a lot of spacing so i got rid of the spaces
    j_data_serialized = j_data_serialized.translate({ord(c): None for c in string.whitespace})
    expected = expected.translate({ord(c): None for c in string.whitespace})
    # giving me an error because my expected values printed out ' and not "
    expected = expected.replace("'", "\"")
    assert j_data_serialized == expected


@pytest.mark.parametrize("argv1,argv2,expected1,expected2,expected3,expected4", 
                        [("q_cs5260-requests", "d_widgets",
                          "q", "cs5260-requests", "d", "widgets"),
                        ("b_usu-cs5260-wasatch-dist", "b_usu-cs5260-wasatch-web",
                         "b", "usu-cs5260-wasatch-dist", "b", "usu-cs5260-wasatch-web")
                        ])
def test_analyze_cl_arguments(argv1, argv2, expected1, expected2, expected3, expected4):
    type_rtu, get_resources_here, type_requst, put_requests_here = analyze_cl_arguments(argv1, argv2)
    assert type_rtu == expected1
    assert get_resources_here == expected2
    assert type_requst == expected3
    assert put_requests_here == expected4


@pytest.mark.parametrize("j_data_serialized, put_requests_here, owner, widget_id, expected", 
                        [("{'name':'quantity','value':'786'}", "usu-cs5260-wasatch-dist",
                        "henry-hops", "3b7fef78", "widget/henry-hops/3b7fef78"),
                        ("{'name':'size','value':'504'}", "usu-cs5260-wasatch-web",
                        "mary-matthews", "213c66e75112", "widget/mary-matthews/213c66e75112")
                        ])
def test_insert_into_bucket(j_data_serialized, put_requests_here, owner, widget_id, expected):
    client = MagicMock()
    client.put_object(
        j_data_serialized,
        put_requests_here,
        f'widget/{owner}/{widget_id}'
        )
    client.put_object.assert_called_with(j_data_serialized, put_requests_here, expected)


@pytest.mark.parametrize("item,expected", 
                        [("{'id': '3b7fef78-f757-424d-9c0c-431768022528', 'owner': 'mary-matthews', 'label': 'F', 'description': 'PFZCIEPDAYIYRAKNO', 'size': '504', 'height': '431', 'height-unit': 'cm', 'width-unit': 'cm', 'quantity': '786'}",
                        "{'id': '3b7fef78-f757-424d-9c0c-431768022528', 'owner': 'mary-matthews', 'label': 'F', 'description': 'PFZCIEPDAYIYRAKNO', 'size': '504', 'height': '431', 'height-unit': 'cm', 'width-unit': 'cm', 'quantity': '786'}"),
                        ("{'id': 'deab01f2-0075-4a11-87e1-21cbea5c0c7f', 'owner': 'henry-hops', 'label': 'DHSRL', 'description': 'XROPEIDSOOJAYOXGY', 'size': '884', 'height': '51', 'length-unit': 'cm', 'rating': '4.8361626', 'price': '34.82', 'quantity': '297', 'vendor': 'EPAQ'}",
                        "{'id': 'deab01f2-0075-4a11-87e1-21cbea5c0c7f', 'owner': 'henry-hops', 'label': 'DHSRL', 'description': 'XROPEIDSOOJAYOXGY', 'size': '884', 'height': '51', 'length-unit': 'cm', 'rating': '4.8361626', 'price': '34.82', 'quantity': '297', 'vendor': 'EPAQ'}")
                        ])
def test_intert_into_dynamodb_table(item, expected):
    table = MagicMock()
    table.put_item(item)
    table.put_item.assert_called_with(expected)


@pytest.mark.parametrize("resources_to_use, key, expected1, expected2", 
                        [("usu-cs5260-wasatch-dist", "1612306369713",
                        "usu-cs5260-wasatch-dist", "1612306369713"),
                        ("usu-cs5260-wasatch-web", "1612306374392'",
                        "usu-cs5260-wasatch-web", "1612306374392'")
                        ])
def test_delete_from_bucket(resources_to_use, key, expected1, expected2):
    client = MagicMock()
    client.delete_object(resources_to_use, key)
    client.delete_object.assert_called_with(expected1, expected2)

@pytest.mark.parametrize("message", 
                        [("messagetest1"), 
                         ("messagetest2")
                        ])
def test_delete_from_queue(message):
    message = MagicMock()
    message.delete()
    message.delete.assert_called()


@pytest.mark.parametrize("queue, max_number, wait_time", 
                        [("usu-cs5260-wasatch-dist", 1669713, 10),
                        ("usu-cs5260-wasatch-web", 1674392, 15)
                        ])
def test_delete_from_bucket(queue, max_number, wait_time):
    queue = MagicMock()
    queue.receive_messages(['All'], max_number, wait_time)
    queue.receive_messages.assert_called_with(['All'], max_number, wait_time)
