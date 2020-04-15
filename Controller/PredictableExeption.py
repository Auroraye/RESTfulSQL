class PredictableException(Exception):
    def __init__(self, input):
        self.massage = input

    def handle_me(self):
        return self.massage


class PredictableUnknownKeyException(PredictableException):
    def handle_me(self):
        text = "There is an unknown column in your 'uniques' field, and that is "
        text = text + self.massage
        return text


class PredictableInvalidArgumentException(PredictableException):
    def handle_me(self):
        if self.massage == "1":
            text = "Please fill in the 'table' field."
            return text
        elif self.massage == "2":
            text = "Only one table can be created each time, please remove other tables from the 'table' field."
            return text
        elif self.massage == "3":
            text = "Please check the parentheses in your 'uniques' field."
            return text
        else:
            return self.massage


class PredictableDuplicateColumnException(PredictableException):
    def handle_me(self):
        text = "There is at least one column in your 'columns' field duplicated, and the first one is "
        text = text + self.massage + "."
        return text

