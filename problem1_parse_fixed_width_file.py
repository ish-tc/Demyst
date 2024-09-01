import csv

class ParseFixedWidthFile:
    def __init__(self):
        self.spec_json = {
            "ColumnNames": [
                "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10"
            ],
            "Offsets": [5, 12, 3, 2, 13, 7, 10, 13, 20, 13],
            "FixedWidthEncoding": "windows-1252",
            "IncludeHeader": "True",
            "DelimitedEncoding": "utf-8"
        }
        self.data = [
            ["George", "A", "3000", "MM", "123 st.", "May 30", "Los Angeles", "CA", "90001", "USA"],
            ["Jane", "Smith", "3080", "FM", "456 Oak St.", "Apt 2", "Los Angeles", "CA", "90001", "USA"],
        ]

    def generate_fixed_width_file(self, fixed_width_filename="output_fixed_width_file.txt", delimited_output_file="delimited_output_file.csv"):
        offsets = self.spec_json["Offsets"]
        column_names = self.spec_json["ColumnNames"]
        delimited_encoding = self.spec_json["DelimitedEncoding"]
        fixed_width_encoding = self.spec_json["FixedWidthEncoding"]
        include_header = self.spec_json["IncludeHeader"]

        # Generate the fixed-width file
        with open(fixed_width_filename, "w", encoding=fixed_width_encoding) as f:
            if include_header == "True":
                header = "".join([col.ljust(offset) for col, offset in zip(column_names, offsets)])
                f.write(header + "\n")

            for row in self.data:
                line = "".join([str(item).ljust(offset) for item, offset in zip(row, offsets)])
                f.write(line + "\n")

        # Read the fixed-width file and convert to delimited CSV
        with open(fixed_width_filename, "r", encoding=fixed_width_encoding) as fixed_width_file:
            lines = fixed_width_file.readlines()

        with open(delimited_output_file, "w", newline='', encoding=delimited_encoding) as csv_file:
            writer = csv.writer(csv_file)

            if include_header == "True":
                writer.writerow(column_names)
                data_lines = lines[1:]  # Skip the header
            else:
                data_lines = lines

            for line in data_lines:
                row = []
                start = 0
                for offset in offsets:
                    field = line[start:start + int(offset)].strip()
                    row.append(field)
                    start += int(offset)
                writer.writerow(row)

# Instantiation and method call
parse_fixed_width_file_obj = ParseFixedWidthFile()
parse_fixed_width_file_obj.generate_fixed_width_file()