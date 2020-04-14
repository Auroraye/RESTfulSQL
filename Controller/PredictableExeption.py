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
        if self.massage == "2":
            text = "Please chech the parentheses in your 'uniques' field."
            return text
        else:
            return self.massage
