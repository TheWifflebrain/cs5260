from consumer import *
import pytest
import string

@pytest.mark.parametrize("body,expected1, expected2, expected3", 
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


# @pytest.mark.parametrize("json_data,owner,expected", 
#                         [(namespace(description='PFZCIEPDAYIYRAKNO', label='F', otherAttributes=[namespace(name='size', value='504'), namespace(name='height', value='431'), namespace(name='height-unit', value='cm'), namespace(name='width-unit', value='cm'), namespace(name='quantity', value='786')], owner='Mary Matthews', requestId='b897b7f0-ffba-4960-a611-213c66e75112', type='create', widgetId='3b7fef78-f757-424d-9c0c-431768022528'),
#                         "mary-matthews",
#                         {'id': '3b7fef78-f757-424d-9c0c-431768022528', 'owner': 'mary-matthews', 'label': 'F', 'description': 'PFZCIEPDAYIYRAKNO', 'size': '504', 'height': '431', 'height-unit': 'cm', 'width-unit': 'cm', 'quantity': '786'}
#                         ),

#                         ])
# def test_prepare_dynamodb_data(json_data, owner, expected):
#     item = prepare_dynamodb_data(json_data, owner)
#     assert item == expected

@pytest.mark.parametrize("body,expected", 
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


@pytest.mark.parametrize("argv1,argv2,expected1,expected2,expected3", 
                        [("usu-cs5260-wasatch-requests", "d_widgets",
                        "usu-cs5260-wasatch-requests", "d", "widgets"),
                        ("usu-cs5260-wasatch-dist", "b_usu-cs5260-wasatch-web",
                        "usu-cs5260-wasatch-dist", "b", "usu-cs5260-wasatch-web")
                        ])
def test_analyze_cl_arguments(argv1, argv2, expected1, expected2, expected3):
    resources_to_use, type_requst, put_requests_here = analyze_cl_arguments(argv1, argv2)
    assert resources_to_use == expected1
    assert type_requst == expected2
    assert put_requests_here == expected3
