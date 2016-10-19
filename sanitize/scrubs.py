import json
import re

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
                       "sandboxCredentials",
                       "sharedSecret",
                       "state",
                       "street1",
                       "street2",
                       "zip"]

INT_FIELDS_TO_SCRUB = ["expirationMonth", "expirationYear"]


def scrub(data):
    json_data = json.dumps(data)
    new_json_data = re.sub(
        pattern=r'"({})":.*?"(.+?)"'.format('|'.join(STR_FIELDS_TO_SCRUB)),
        repl=r'"\1": "***"',
        string=json_data)
    new_json_data = re.sub(
        pattern=r'"({})":.*?(\d*).*?,'.format('|'.join(INT_FIELDS_TO_SCRUB)),
        repl=r'"\1": 0,',
        string=new_json_data)

    return json.loads(new_json_data)
