import csv
import requests

def upload_file(table, url, mysql):
	if not url.endsWith("csv"):
		return 400, None, None, "The file url is not valid."
	
	with requests.Session() as s:
		# download = s.get("http://samplecsvs.s3.amazonaws.com/Sacramentorealestatetransactions.csv")
		download = s.get(url)
		decoded_content = download.content.decode('utf-8')
		cr = csv.reader(decoded_content.splitlines(), delimiter=',')
		my_list = list(cr)
		column_names = my_list[0]
		if "Error" in my_list[1]:
			return 400, None, None, "Failed to download the file"
		
		# Create table 

		# Upload data
		for row in my_list[1:]:
			print(row)
	
	return "OK", 200