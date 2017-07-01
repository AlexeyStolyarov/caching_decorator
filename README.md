# caching_decorator
Caching decorator for caching results returned by functions

## Usage:


@cashe_to_db(timedelta)   
def function_that_you_want_to_cashe:   
    pass  


if you run this script directly it starts unittests

## parameters 

### DB_MAX_ITEMS
### timedelta:
- DB_DATA_TTL 	= timedelta(days=0, hours=0, minutes=0, seconds=10)
- DB_DATA_TTL_5M 	= timedelta(days=0, hours=0, minutes=5, seconds=0)
- DB_DATA_TTL_1H	= timedelta(days=0, hours=1, minutes=0, seconds=0)
- DB_DATA_TTL_1D	= timedelta(days=1, hours=0, minutes=0, seconds=0)
- DB_DATA_TTL_7D	= timedelta(days=7, hours=0, minutes=0, seconds=0)

