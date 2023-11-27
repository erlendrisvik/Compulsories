class InvalidYearError(Exception):
    def __init__(self, message="Invalid year"):
        self.message = message
        super().__init__(self.message)

class DataExistsError(Exception):
    def __init__(self, message="Data already exists in database"):
        self.message = message
        super().__init__(self.message)

class FetchDataError(Exception):
    def __init__(self, message="Error fetching data"):
        self.message = message
        super().__init__(self.message)

class WritingToDatabaseError(Exception):
    def __init__(self, message="Error writing to database"):
        self.message = message
        super().__init__(self.message)

class NoDataError(Exception):
    def __init__(self, message="No data in API"):
        self.message = message
        super().__init__(self.message)