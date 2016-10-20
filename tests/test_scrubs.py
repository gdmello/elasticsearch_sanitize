from nose.tools import eq_

from sanitize import scrubs

data = {
    "doc": {
        "status": "complete",
        "links": {
            "self": {
                "href": "http://BASE_URL_PLACEHOLDER-f1a763a6-ff79-47bc-bca3-8c2867479a8e.com/orders/e58eba55-72c1-4edd-a4ba-8059b89cc389"
            }
        },
        "fulfillment": "https://sandbox-staging.lcp.points.com/v1/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/buy/",
        "updates": [
            {
                "status": "success",
                "type": "memberValidation",
                "resource": "https://sandbox-staging.lcp.points.com/v1/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/mvs/858c14d1-b617-4522-8cf1-1bd5ae336bb6",
                "updatedAt": "2015-01-20T14:45:57.526271Z"
            },
            {
                "status": "authorized",
                "type": "payment",
                "resource": "https://sandbox-staging.lcp.points.com/v1/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/payments/3b792159-007d-4961-bf0a-fe7959e3406a",
                "updatedAt": "2015-01-20T14:45:58.086595Z"
            },
            {
                "status": "success",
                "type": "credit",
                "resource": "https://sandbox-staging.lcp.points.com/v1/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/credits/aad7c848-80a9-4057-a864-d36cc5faae27",
                "updatedAt": "2015-01-20T14:45:58.841995Z"
            },
            {
                "status": "captured",
                "type": "payment",
                "resource": "https://sandbox-staging.lcp.points.com/v1/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/payments/3b792159-007d-4961-bf0a-fe7959e3406a",
                "updatedAt": "2015-01-20T14:45:59.336275Z"
            }
        ],
        "updatedAt": "2015-01-20T14:45:59.473771Z",
        "confirmationNumber": "0419-7798-7116-2290-3071",
        "data": {
            "sessionLog": "",
            "offerSet": "https://sandbox-staging.lcp.points.com/v1/offer-sets/ef37a2f6-b927-430a-af91-61b998ced7c9",
            "loyaltyProgram": "https://sandbox-staging.lcp.points.com/v1/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d",
            "clientIpAddress": "1.1.1.1",
            "orderDetails": {
                "basePoints": 40000,
                "offer": "https://sandbox-staging.lcp.points.com/v1/offers/af7a999a-f300-4ad3-b213-496a2982ce95",
                "pics": {
                    "bonus": "123",
                    "base": "123"
                },
                "retailRate": "0.007",
                "bonusPoints": 30000
            },
            "clientUserAgent": "Browser data",
            "payment": {
                "currency": "USD",
                "costs": {
                    "baseCost": 280,
                    "totalCost": 280,
                    "taxes": [
                        {
                            "amount": 0,
                            "name": "GST/HST"
                        }
                    ],
                    "fees": []
                },
                "type": "creditCard",
                "billingInfo": {
                    "expirationMonth": 12,
                    "cardName": "Club Carlson Visa",
                    "street1": "171 John St",
                    "street2": "5th floor",
                    "phone": "4165551234",
                    "expirationYear": 2030,
                    "cardNumber": "XXXXXXXXXXXX1111",
                    "securityCode": "XXX",
                    "city": "Toronto",
                    "zip": "M8V 1S1",
                    "firstName": "John",
                    "country": "GB",
                    "cardType": "AMEX",
                    "state": "XX",
                    "lastName": "Smith"
                }
            },
            "user": {
                "status": "success",
                "links": {
                    "self": {
                        "href": "https://sandbox-staging.lcp.points.com/v1/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/mvs/858c14d1-b617-4522-8cf1-1bd5ae336bb6"
                    }
                },
                "memberValidation": "https://sandbox-staging.lcp.points.com/v1/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d/mvs/858c14d1-b617-4522-8cf1-1bd5ae336bb6",
                "updatedAt": "2015-01-20T14:45:56.255528Z",
                "createdAt": "2015-01-20T14:45:56.042470Z",
                "loyaltyProgram": "https://sandbox-staging.lcp.points.com/v1/lps/ef7ddc8a-3a81-48f7-9e50-d3545de2a18d",
                "membershipLevel": "G1",
                "firstName": "Opal",
                "lastName": "Test 0",
                "memberId": "2629",
                "application": "https://sandbox-staging.lcp.points.com/v1/apps/fc89c7aa-2fb1-4ec5-8a96-bdb093dad703",
                "balance": 0,
                "type": "memberValidation",
                "email": "qa@points.com"
            }
        },
        "createdAt": "2015-01-20T14:45:57.268257Z",
        "orderType": "BUY",
        "application": "http://BASE_URL_PLACEHOLDER-f1a763a6-ff79-47bc-bca3-8c2867479a8e.com/apps/fc89c7aa-2fb1-4ec5-8a96-bdb093dad703",
        "_permissions": [
            {
                "resource": "/orders/e58eba55-72c1-4edd-a4ba-8059b89cc389",
                "mode": "account",
                "links": {
                    "self": {
                        "href": "/security/permission-sets/22a0fe72-4691-4a46-9320-2a8cbf8f2ee6"
                    }
                },
                "groupPermissions": [
                    {
                        "group": "/security/groups/77745fe3-7a43-40d7-a7fe-4254ee19c3c6",
                        "permissions": [
                            "GET"
                        ]
                    },
                    {
                        "group": "/security/groups/3b181cf6-3af0-4238-a455-168f1eb77d38",
                        "permissions": [
                            "GET"
                        ]
                    }
                ],
                "updatedAt": "2015-01-20T14:45:57.422587Z",
                "ownerPermissions": {
                    "principal": "/security/principals/a5d6bd36-f110-4b91-87db-3120d8b2d4cb",
                    "permissions": []
                },
                "type": "permissionSet",
                "createdAt": "2015-01-20T14:45:57.422587Z"
            },
            {
                "resource": "/orders/e58eba55-72c1-4edd-a4ba-8059b89cc389",
                "links": {
                    "self": {
                        "href": "/security/permission-sets/359a1306-8ba6-4591-8036-20167b1dbc00"
                    }
                },
                "ownerPermissions": {
                    "principal": "/security/principals/a5d6bd36-f110-4b91-87db-3120d8b2d4cb",
                    "permissions": [
                        "GET",
                        "POST"
                    ]
                },
                "mode": "sandbox",
                "updatedAt": "2015-01-20T14:45:57.305656Z",
                "type": "permissionSet",
                "createdAt": "2015-01-20T14:45:57.305656Z"
            }
        ],
        "type": "order",
        "metadata": {
            "baseUrlPlaceholder": "http://BASE_URL_PLACEHOLDER-f1a763a6-ff79-47bc-bca3-8c2867479a8e.com",
            "mode": "sandbox"
        }
    },
    "_type": "order",
    "_id": "e58eba55-72c1-4edd-a4ba-8059b89cc389",
    "_op_type": "create",
    "_index": "lcp_v2_copy"
}

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


# def test_scrub():
#     eq_(expected_data, scrubs.scrub(data))


def test_handles_trailing_comma():
    data = [{"billingInfo": {
        "expirationMonth": 12,
        "cardName": "Club Carlson Visa",
        "street1": "171 John St",
        "street2": "5th floor",
        "phone": "4165551234",
        "expirationYear": 2030,
        "cardNumber": "XXXXXXXXXXXX1111",
        "securityCode": "XXX",
        "city": "Toronto",
        "zip": "M8V 1S1",
        "firstName": "John",
        "country": "GB",
        "cardType": "AMEX",
        "state": "XX",
        "lastName": "Smith",
        "someField": "someValue"
    }}]
    expected_data = [{"billingInfo": {
        "expirationMonth": 0,
        "cardName": "***",
        "street1": "***",
        "street2": "***",
        "phone": "***",
        "expirationYear": 0,
        "cardNumber": "***",
        "securityCode": "XXX",
        "city": "***",
        "zip": "***",
        "firstName": "***",
        "country": "***",
        "cardType": "***",
        "state": "***",
        "lastName": "***",
        "someField": "someValue"
    }}]
    eq_(expected_data, scrubs.scrub(data))


def test_hanldes_no_trailing_comma():
    pass
