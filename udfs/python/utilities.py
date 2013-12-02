from pig_util import outputSchema

COUNT = 0

@outputSchema('auto_increment_id:int')
def auto_increment_id():
    global COUNT
    COUNT += 1
    return COUNT