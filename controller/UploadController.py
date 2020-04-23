import csv
import requests

from controller.TableController import create_table
from controller.TabledataController import vanilla_post_tabledata

def upload_file(table, url, mysql):
	if not url.endswith(".csv"):
		return 400, None, None, "The file url is not valid. Must ends with .csv."
	
	with requests.Session() as s:
		download = s.get(url)
		decoded_content = download.content.decode('utf-8')
		cr = csv.reader(decoded_content.splitlines(), delimiter=',')
		my_list = list(cr)
		column_names = ','.join(my_list[0])
		if "Error" in my_list[1]:
			return 400, None, None, "Failed to download the file"
		
		# Create table 
		status, message, data, error = create_table(table, column_names, "", mysql)
		if (status == 401):
			return 401, None, None, "Please connect to a database using the /connect endpoint."
		
		# Upload data
		for row in my_list[1:2]:
			row_list = ','.join(row)
			print(column_names)
			print(row_list)
			status, message, data, error = vanilla_post_tabledata(table, column_names, row_list, mysql)
	
	return 200, "Table Created", None, None