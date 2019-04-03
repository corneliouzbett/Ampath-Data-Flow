
def connection(connection):
    print(connection)

class Mysql():
    def __init__(self, name,config):
        self.name = name
        self.config = config

    def getMysqlContext(self):
        # create connection to mysql database