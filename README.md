```
 / __/ /__ ____ / /_(_)___/ __/__ ___ _________/ / 
 / _// / _ `(_-</ __/ / __/\ \/ -_) _ `/ __/ __/ _ \
/___/_/\_,_/___/\__/_/\__/___/\__/\_,_/_/  \__/_//_/
                                                    
   ____          _ __  _            
  / __/__ ____  (_) /_(_)__ ___ ____
 _\ \/ _ `/ _ \/ / __/ /_ // -_) __/
/___/\_,_/_//_/_/\__/_//__/\__/_/
```

Sanitize documents stored in an Elasticsearch index for compliance/ obfuscation/ privacy protection.

Requirements
============
* Python
* Pip
* Virtualenv

Installation
============
* Create a virtualenv
    mkvirtualenv sanitizer
* Install requirements
    pip install -r requirements.txt
    
Execute
=======
* Run sanitization
    $ python sanitize/main.py --user some_user --password some_password \
    --source http://my_elasticsearch_host:9200/ --destination_user dest_user \
    -- destination_password dest_password \
    --destination http://my_new_elasticsearch_host:9200/
