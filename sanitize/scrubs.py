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
                       "memberId",
                       "organizationName",
                       "password",
                       "phone",
                       "sharedSecret",
                       "state",
                       "street1",
                       "street2",
                       "zip"]

INT_FIELDS_TO_SCRUB = ["expirationMonth", "expirationYear"]
LIST_FIELDS_TO_SCRUB = ["liveCredentials", "sandboxCredentials"]


def scrub(data):
    json_data = json.dumps(data)
    new_json_data=''
    try:
        new_json_data = re.sub(
            pattern=r'"({})":\s*?\[(.*?)\].*?(,?)'.format('|'.join(LIST_FIELDS_TO_SCRUB)),
            repl=r'"\1": ["***"]\3',
            string=json_data,
            flags=re.DOTALL)
        new_json_data = re.sub(
            pattern=r'"({})":.*?(".*?")'.format('|'.join(STR_FIELDS_TO_SCRUB)),
            repl=r'"\1": "***"',
            string=new_json_data)
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

