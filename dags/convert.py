import json
# def process_json(obj):
#     for key in obj:
        # Kiểm tra xem giá trị có phải là mảng hay không
        # if isinstance(obj[key], list) and len(obj[key]) > 0:
        #     # Nếu là mảng, thì lấy phần tử đầu tiên
        #     obj[key] = obj[key][0]
        # if isinstance(obj[key], str) and '\n' in obj[key]:
        #     # Thay thế kí tự '\n' bằng dấu cách
        #     obj[key] = obj[key].replace('\n', ' ')



# Đọc nội dung từ tập tin JSON

# Ghi lại nội dung đã được xử lý vào tập tin JSON ban đầu
# with open(file_path, 'w') as file:
#     json.dump(data, file, indent=2, ensure_ascii=False)

# import json

# def jsonl_to_json(jsonl_file, json_file):
#     # Mở tập tin JSONL để đọc
#     with open(jsonl_file, 'r') as infile:
#         # Đọc từng dòng và chuyển đối tượng JSONL thành danh sách
#         json_list = [json.loads(line) for line in infile]

#     # Ghi danh sách đối tượng JSON vào tập tin JSON
#     with open(json_file, 'w') as outfile:
#         json.dump(json_list, outfile, indent=2, ensure_ascii=False)

# # Thực hiện chuyển đổi từ file JSONL sang JSON
# jsonl_to_json('companyList.jsonl', 'companies.json')