from flask import jsonify
resultlist = []

from flask import jsonify

# item = ('dname', 'char(20)', 'NO', 'UNI', None, '')
# print(jsonify(Field=item[0], Type=item[1], Null=item[2], Key=item[3]))
#
# resultlist.append([{'Field': item[0],
#                    'Type': item[1],
#                    'Null': item[2],
#                    'Key': item[3]}])

# print(resultlist)

# resultlist = []
# for item in result:
#     print(item[0])
#     resultlist.append([{'Field': item[0], 'Type': item[1], 'Null': item[2], 'Key': item[3]}])
# print(resultlist)
# return jsonify(resultlist), 200

result = (('dname', 'char(20)', 'NO', 'UNI', None, ''), ('dname', 'char(20)', 'NO', 'UNI', None, ''), ('dname', 'char(20)', 'NO', 'UNI', None, ''))
# print(user for user in result)
result[1] = ('ffe','fefe')
print(result[1])
