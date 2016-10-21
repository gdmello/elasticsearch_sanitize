import json

from nose.tools import eq_

from sanitize import scrubs

data = [
    {
        "doc": {
            "status": "success",
            "memberValidation": "http://BASE_URL_PLACEHOLDER-eb0baf12-a301-4e7d-85ba-8395ac6a4478.com/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/mvs/025216f8-44ed-4f06-b7e4-1bba8ac7ba50",
            "loyaltyProgram": "http://BASE_URL_PLACEHOLDER-eb0baf12-a301-4e7d-85ba-8395ac6a4478.com/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d",
            "links": {
                "self": {
                    "href": "http://BASE_URL_PLACEHOLDER-eb0baf12-a301-4e7d-85ba-8395ac6a4478.com/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/credits/97ee1aad-b2a0-4461-ada3-b6c422301a5c"
                }
            },
            "pic": "123",
            "application": "http://BASE_URL_PLACEHOLDER-eb0baf12-a301-4e7d-85ba-8395ac6a4478.com/apps/fc89c7aa-2fb1-4ec5-8a96-bdb093dad703",
            "amount": 70000,
            "updatedAt": "2015-01-20T14:45:44.538437Z",
            "transactionId": "7081017",
            "type": "credit",
            "order": "http://BASE_URL_PLACEHOLDER-eb0baf12-a301-4e7d-85ba-8395ac6a4478.com/orders/b09575ab-81a9-4fd5-8bda-6925d6c17ea8",
            "createdAt": "2015-01-20T14:45:44.301088Z"
        },
        "_type": "credit",
        "_id": "97ee1aad-b2a0-4461-ada3-b6c422301a5c",
        "_op_type": "create",
        "_index": "lcp_v2_copy"
    },
    {
        "doc": {
            "links": {
                "captures": {
                    "href": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/lps/cbb8b5bc-db22-430b-8508-b68ffd0452f0/payments/35ed1d8e-cecc-4469-b6ce-c517acbb3ba9/captures/"
                },
                "self": {
                    "href": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/lps/cbb8b5bc-db22-430b-8508-b68ffd0452f0/payments/35ed1d8e-cecc-4469-b6ce-c517acbb3ba9"
                }
            },
            "clientUserAgent": "Browser data",
            "captures": [
                {
                    "status": "success",
                    "createdAt": "2014-07-29T19:42:03.692000Z",
                    "links": {
                        "self": {
                            "href": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/lps/cbb8b5bc-db22-430b-8508-b68ffd0452f0/payments/35ed1d8e-cecc-4469-b6ce-c517acbb3ba9/captures/1"
                        }
                    },
                    "updatedAt": "2015-04-22T18:33:10.143291Z"
                }
            ],
            "memberValidation": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/lps/cbb8b5bc-db22-430b-8508-b68ffd0452f0/mvs/9934006f-2e1b-48ac-97cb-a689e2dda666",
            "currency": "USD",
            "updatedAt": "2015-04-22T18:33:10.146241Z",
            "createdAt": "2014-07-29T19:42:01.094000Z",
            "baseCost": 312,
            "loyaltyProgram": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/lps/cbb8b5bc-db22-430b-8508-b68ffd0452f0",
            "billingInfo": {
                "expirationMonth": 12,
                "city": "Toronto",
                "zip": "M8V 1S1",
                "firstName": "Svvd",
                "country": "US",
                "street2": "5th floor",
                "expirationYear": 2030,
                "phone": "4165551234",
                "state": "CA",
                "cardNumber": "411111XXXXXX1111",
                "lastName": "Ymqrw",
                "street1": "171 John St",
                "email": "ohnudlzkm@points.com",
                "cardType": "MASTERCARD"
            },
            "upstreamDetails": {
                "providerOrderId": "2866372400000000"
            },
            "application": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/apps/fc89c7aa-2fb1-4ec5-8a96-bdb093dad703",
            "clientIpAddress": "1.1.1.1",
            "type": "payment",
            "transactionType": "Buy",
            "status": "captured",
            "refunds": [],
            "amount": 312,
            "points": 6000,
            "authStatus": "success",
            "order": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/orders/9f4e07dc-bb7b-47ad-a9e5-483d00ebf21f"
        },
        "_type": "payment",
        "_id": "35ed1d8e-cecc-4469-b6ce-c517acbb3ba9",
        "_op_type": "create",
        "_index": "lcp_v2_copy"
    }
]

expected_data = [
    {
        "doc": {
            "status": "success",
            "memberValidation": "http://BASE_URL_PLACEHOLDER-eb0baf12-a301-4e7d-85ba-8395ac6a4478.com/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/mvs/025216f8-44ed-4f06-b7e4-1bba8ac7ba50",
            "loyaltyProgram": "http://BASE_URL_PLACEHOLDER-eb0baf12-a301-4e7d-85ba-8395ac6a4478.com/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d",
            "links": {
                "self": {
                    "href": "http://BASE_URL_PLACEHOLDER-eb0baf12-a301-4e7d-85ba-8395ac6a4478.com/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/credits/97ee1aad-b2a0-4461-ada3-b6c422301a5c"
                }
            },
            "pic": "123",
            "application": "http://BASE_URL_PLACEHOLDER-eb0baf12-a301-4e7d-85ba-8395ac6a4478.com/apps/fc89c7aa-2fb1-4ec5-8a96-bdb093dad703",
            "amount": 70000,
            "updatedAt": "2015-01-20T14:45:44.538437Z",
            "transactionId": "7081017",
            "type": "credit",
            "order": "http://BASE_URL_PLACEHOLDER-eb0baf12-a301-4e7d-85ba-8395ac6a4478.com/orders/b09575ab-81a9-4fd5-8bda-6925d6c17ea8",
            "createdAt": "2015-01-20T14:45:44.301088Z"
        },
        "_type": "credit",
        "_id": "97ee1aad-b2a0-4461-ada3-b6c422301a5c",
        "_op_type": "create",
        "_index": "lcp_v2_copy"
    },
    {
        "doc": {
            "links": {
                "captures": {
                    "href": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/lps/cbb8b5bc-db22-430b-8508-b68ffd0452f0/payments/35ed1d8e-cecc-4469-b6ce-c517acbb3ba9/captures/"
                },
                "self": {
                    "href": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/lps/cbb8b5bc-db22-430b-8508-b68ffd0452f0/payments/35ed1d8e-cecc-4469-b6ce-c517acbb3ba9"
                }
            },
            "clientUserAgent": "***",
            "captures": [
                {
                    "status": "success",
                    "createdAt": "2014-07-29T19:42:03.692000Z",
                    "links": {
                        "self": {
                            "href": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/lps/cbb8b5bc-db22-430b-8508-b68ffd0452f0/payments/35ed1d8e-cecc-4469-b6ce-c517acbb3ba9/captures/1"
                        }
                    },
                    "updatedAt": "2015-04-22T18:33:10.143291Z"
                }
            ],
            "memberValidation": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/lps/cbb8b5bc-db22-430b-8508-b68ffd0452f0/mvs/9934006f-2e1b-48ac-97cb-a689e2dda666",
            "currency": "USD",
            "updatedAt": "2015-04-22T18:33:10.146241Z",
            "createdAt": "2014-07-29T19:42:01.094000Z",
            "baseCost": 312,
            "loyaltyProgram": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/lps/cbb8b5bc-db22-430b-8508-b68ffd0452f0",
            "billingInfo": {
                "expirationMonth": 0,
                "city": "***",
                "zip": "***",
                "firstName": "***",
                "country": "***",
                "street2": "***",
                "expirationYear": 0,
                "phone": "***",
                "state": "***",
                "cardNumber": "***",
                "lastName": "***",
                "street1": "***",
                "email": "***",
                "cardType": "***"
            },
            "upstreamDetails": {
                "providerOrderId": "2866372400000000"
            },
            "application": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/apps/fc89c7aa-2fb1-4ec5-8a96-bdb093dad703",
            "clientIpAddress": "***",
            "type": "payment",
            "transactionType": "Buy",
            "status": "captured",
            "refunds": [],
            "amount": 312,
            "points": 6000,
            "authStatus": "success",
            "order": "http://BASE_URL_PLACEHOLDER-ce78ff63-9a53-45c8-94ad-7e3ad3efe2dc.com/orders/9f4e07dc-bb7b-47ad-a9e5-483d00ebf21f"
        },
        "_type": "payment",
        "_id": "35ed1d8e-cecc-4469-b6ce-c517acbb3ba9",
        "_op_type": "create",
        "_index": "lcp_v2_copy"
    }
]


def test_scrub():
    eq_(expected_data, scrubs.scrub(data))


def test_handles_str_fields_with_trailing_comma():
    data_to_anonymize = [{"billingInfo": {
        "expirationMonth": 12,
        "state": "XX",
        "lastName": "Smith",
        "someField": "someValue"
    }}]
    expected_data_to_anonymize = [{"billingInfo": {
        "expirationMonth": 0,
        "state": "***",
        "lastName": "***",
        "someField": "someValue"
    }}]
    eq_(expected_data_to_anonymize, scrubs.scrub(data_to_anonymize))


def test_handles_str_fields_with_no_trailing_comma():
    data_to_anonymize = [{"billingInfo": {
        "expirationMonth": 12,
        "cardType": "AMEX",
        "state": "XX",
        "lastName": "Smith"
    }}]
    expected_data_to_anonymize = [{"billingInfo": {
        "expirationMonth": 0,
        "cardType": "***",
        "state": "***",
        "lastName": "***"
    }}]
    eq_(expected_data_to_anonymize, scrubs.scrub(data_to_anonymize))


def test_handles_empty_str_fields_with_trailing_comma():
    data_to_anonymize = [{"billingInfo": {
        "expirationMonth": 12,
        "state": "XX",
        "lastName": "",
        "someField": "someValue"
    }}]
    expected_data_to_anonymize = [{"billingInfo": {
        "expirationMonth": 0,
        "state": "***",
        "lastName": "***",
        "someField": "someValue"
    }}]
    eq_(expected_data_to_anonymize, scrubs.scrub(data_to_anonymize))


def test_handles_empty_str_fields_with_no_trailing_comma():
    data_to_anonymize = [{"billingInfo": {
        "expirationMonth": 12,
        "state": "XX",
        "lastName": ""
    }}]
    expected_data_to_anonymize = [{"billingInfo": {
        "expirationMonth": 0,
        "state": "***",
        "lastName": "***"
    }}]
    eq_(expected_data_to_anonymize, scrubs.scrub(data_to_anonymize))


def test_handles_int_fields_with_no_trailing_comma():
    data_to_anonymize = [{"billingInfo": {
        "state": "XX",
        "lastName": "Smith",
        "expirationMonth": 12,
        "someField": "someValue"
    }}]
    expected_data_to_anonymize = [{"billingInfo": {
        "state": "***",
        "lastName": "***",
        "expirationMonth": 0,
        "someField": "someValue"
    }}]
    eq_(expected_data_to_anonymize, scrubs.scrub(data_to_anonymize))


def test_handles_int_fields_with_trailing_comma():
    data_to_anonymize = [{"billingInfo": {
        "state": "XX",
        "lastName": "Smith",
        "expirationMonth": 12
    }}]
    expected_data_to_anonymize = [{"billingInfo": {
        "state": "***",
        "lastName": "***",
        "expirationMonth": 0
    }}]
    eq_(expected_data_to_anonymize, scrubs.scrub(data_to_anonymize))


def test_handles_empty_credentials():
    data_to_anonymize = [{
        "sandboxCredentials": [
            "http://BASE_URL_PLACEHOLDER-3dfd2e87-126f-47af-a0aa-3761316a0496.com/123"
        ],
        "name": "ConsoleE2EApp1cqchc06nf",
        "liveCredentials": [], }]
    expected_data_to_anonymize = [{
        "sandboxCredentials": ["***"],
        "name": "ConsoleE2EApp1cqchc06nf",
        "liveCredentials": ["***"], }]
    eq_(expected_data_to_anonymize, scrubs.scrub(data_to_anonymize))


def test_handles_():
    data_to_anonymize = json.loads('''[{
        "sandboxCredentials": [
            "http://BASE_URL_PLACEHOLDER-3dfd2e87-126f-47af-a0aa-3761316a0496.com/123"
        ],
        "clientIpAddress": null,
        "clientUserAgent": null }]''')
    expected_data_to_anonymize = [{
        "sandboxCredentials": ["***"],
        "clientIpAddress": "***",
        "clientUserAgent": "***"}]
    eq_(expected_data_to_anonymize, scrubs.scrub(data_to_anonymize))

# def test_test():
#     with open('logs/failures/scrub_failures_12a165ca-7efc-4ef8-8998-dac4d84ca92f.json', 'r') as f:
#         data = f.read()
#     import json
#     import ipdb
#     ipdb.set_trace()
#     json_data = json.loads(data)
#     for item in json_data:
#         try:
#             scrubs.scrub(item)
#         except Exception as e:
#             ipdb.set_trace()
#             print json.dumps(item)
