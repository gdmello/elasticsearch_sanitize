STR_FIELDS_TO_SCRUB = ["cardName",
                       "cardNumber",
                       "cardType",
                       "city",
                       "clientIpAddress",
                       "clientSessionId",
                       "clientUserAgent",
                       "country",
                       "email",
                       "firstName",
                       "keyIdentifier",
                       "lastName",
                       "liveCredentials",
                       "memberId",
                       "organizationName",
                       "password",
                       "phone",
                       "sharedSecret",
                       "state",
                       "street1",
                       "street2",
                       "zip",
                       "sandboxCredentials"]

INT_FIELDS_TO_SCRUB = ["expirationMonth", "expirationYear"]


def clean(data_dict):
    for item_dict in data_dict:
        scrub_data(item_dict)
    return data_dict


def scrub_data(data):
    for key, value in data.iteritems():
        if isinstance(value, dict):
            scrub_data(value)
        else:
            if key in STR_FIELDS_TO_SCRUB:
                data[key] = "***"
            elif key in INT_FIELDS_TO_SCRUB:
                data[key] = 0
