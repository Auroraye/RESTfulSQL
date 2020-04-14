class PredictableException(Exception):
    def __init__(self, input):
        self.massage = input

    def handle_me(self):
        return self.massage


class UnknownKeyException(PredictableException):
    def handle_me(self):
        text = "There is an unknown column in your 'uniques' field, and that is "
        text = text + self.massage
        return text
