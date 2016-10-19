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


def scrub(data_dict):
    json_dict = json.dumps(data_dict)
    new_json_dict = re.sub(
        pattern=r'"({})":.*?"(.+?)"'.format('|'.join(STR_FIELDS_TO_SCRUB)),
        repl=r'"\1": "***"',
        string=json_dict)
    new_json_dict = re.sub(
        pattern=r'"({})":.*?(\d*).*?,'.format('|'.join(INT_FIELDS_TO_SCRUB)),
        repl=r'"\1": 0,',
        string=new_json_dict)

    return json.loads(new_json_dict)
