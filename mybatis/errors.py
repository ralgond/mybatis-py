
class Error(Exception):
    def __init__(self, message):
        self.message = message

class DatabaseError(Error):
    def __init__(self, message):
        self.message = message