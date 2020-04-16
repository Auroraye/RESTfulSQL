def organize_return(status, message, data, error):
    return {"message": message}, status

def organize_return_with_data(status, message, data, error):
    return {"message": message, "data": data}, status

def return_response(status, message, data=None, error=None):
	if (error):
		return {"error": error}, 400

	response = {"message": message}
	if (data): 
		response["data"] = data
	
	return response, status