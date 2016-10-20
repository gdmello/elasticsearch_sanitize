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
    try:
        new_json_data = re.sub(
            pattern=r'"({})":.*?"(.*?)"'.format('|'.join(STR_FIELDS_TO_SCRUB)),
            repl=r'"\1": "***"',
            string=json_data)
        new_json_data = re.sub(
            pattern=r'"({})":\s*?(\d*).*?(\n|,)'.format('|'.join(INT_FIELDS_TO_SCRUB)),
            repl=r'"\1": 0\3',
            string=new_json_data)
        nj = json.loads(new_json_data)
    except Exception as e:
        print e
        import ipdb
        ipdb.set_trace()

    return json.loads(new_json_data)
