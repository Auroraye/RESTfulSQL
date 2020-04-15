def organize_return(status, message, data, error):
    return {"message": message}, status


def organize_return_with_data(status, message, data, error):
    return {"message": message, "data": data}, status
