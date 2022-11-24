
"""
pip install bigquery_schema_generator

python3 -m bigquery_schema_generator.generate_schema file.data.jsonile.schema.json.

"""
import json

now 22 x

has change.py 3 generate_scheme from_json.py3

13 14

15

16

17

18

19

20

21

22

23

24

25

26

27

28

29

30

31

32

def generate_schema (json_str, fields = [1, col_name_prefix="", col_type="STRING", col_mode="NULLABLE") :

dict obj = json. loads (json_str)

# print (type (dict obj)) #print (dict_obj)

for key, val in dict obj.items(); #print("key: (key), val: (val)")

if type(val) == dict:

generate_schema (json.dumps (val), col_name_prefix+" "+key, col_type="RECORD") #elif type(val) = list:

#generate_schema (json.dumps (val), col_name prefix+" "+key)

else:

field_schema = {}

if key="text":

field_schema ["name"] = col_name prefix

elif "" in key:

field_schema ["name"] = key.replace("@", col_name_prefix+"/"+"attr_")

else:

field schema ["name"]= col_name prefix+" "+key

field schema ["type"] = col_type field schema ["mode"] = col_mode fields.append(field_schema)

36

37

#print (field schema)

38

39

#print (fields)

40

41

return fields

42

43

44 global fields

45 fields = []

46

def gen schema (json str, col_name prefix="", col_type="STRING", col_mode="NULLABLE"): dict_obj = json. loads (json str)

47

48

49

for key,val in dict_obj.items():

50 51

if type(val) == dict:

field = gen_schema (json.dumps (val), col_name prefix+" "+key)

52

else:

53

field = {}

I

54

55 日

56

517

if key == "text":

field ["name"] = col_name prefix

elif "@" in key:

field ["name"] = key.replace("@", col name prefix+" ")

else:

field["name"] = col_name_prefix+" "+key

58

59

61

62

63

64

if type (val) == dict:

field ["type"] = "RECORD"

else:

field ["type"] = col_type

65

66
if type(val) == list: field ["mode"] = "REPEATED"

else:

field ["mode"] = col_mode

72

73

74

75

if type (val) == dict and field ["type"] == "RECORD":

field ["fields"] = [] field["fields").append(field)

else:

fields.append(field)

76

77

78

return field

79

80

81

82

183

84

def get key_val_from_dict (dict_obj): diet obj = json. loads (dict_obj) for key, val in dict obj.items():

if type (val) == dict:

return get key val from diet (json.dumps (val))

else:

return (key, val)

85

86

87

88

89

90

name main":

91 af

92

93

94

sample_json = '{ "Message": {"Header": "MessageType": ("@MessageVersion": "1.0", "#text": "Status"), "DocumentIdentifier #out generate schema (sample_json)

#print ("out: (out)")

95

96

97

98

out gen schema (sample json)

#print("out: (out)") print("fields: [fields)")

99

100