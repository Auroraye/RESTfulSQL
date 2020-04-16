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
        elif self.massage == "4":
            text = "Only 'default', 'type', and 'nullable' are allowed for 'types' field, please check the field."
            return text
        elif self.massage == "5":
            text = "There is a value that is undefined for the corresponding type in the 'values' field."
            return text
        elif self.massage == "6":
            text = "Only one table can be updated each time, please remove other tables from the 'table' field."
            return text
        elif self.massage == "7":
            text = "Please define at least one key to delete."
            return text
        else:
            return self.massage


class PredictableDuplicateColumnException(PredictableException):
    def handle_me(self):
        text = "There is at least one column in your 'columns' field duplicated, and the first one is "
        text = text + self.massage + "."
        return text


class PredictableTableNotFoundException(PredictableException):
    def handle_me(self):
        text = "The table, '" + self.massage + "' is not found in the database."
        return text


class PredictableNumberOfParameterNotMatchException(PredictableException):
    def handle_me(self):
        list_of_params = self.massage.split(",")
        text = "The parameters, "
        for para in list_of_params:
            text += "'" + para + "', "
        text += "have different number of elements, but they are supposed to have equal number of elements."
        return text


class PredictableConflictOperationException(PredictableException):
    def handle_me(self):
        info = self.massage.split(",")
        text = "There is a conflict/overlapped operation on '" + info[0] + "' " + info[1] + "."
        return text


class PredictableColumnNumberMismatchException(PredictableException):
    def handle_me(self):
        text = "The number of input values and input columns are mismatched."
        return text


class PredictableTypeNotMatchException(PredictableException):
    def handle_me(self):
        fields = self.massage.split(",")
        text = "The value does not match the supposed type. Suppose to have '" + fields[0] + "' typed value,"
        text += "but get value '" + fields[1] + "'."
        return text


class PredictableDuplicateKeyException(PredictableException):
    def handle_me(self):
        text = "There are at least two identical keys in the 'keys' filed, and it is '" + self.massage + "'."
        return text


class PredictableDuplicateConstraintNameException(PredictableException):
    def handle_me(self):
        text = "There are at least two identical key name in the 'key_names' filed, and it is '" + self.massage + "'."
        return text