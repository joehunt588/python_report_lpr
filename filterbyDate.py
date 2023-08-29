import json
import os
from datetime import datetime
from dateutil import parser
from dateutil import tz

#for pdf
from fpdf import FPDF

from tqdm import tqdm  # progress bar
import pandas as pd

file_path_whitelist = '/home/johan/csv-pdf/data/csv/whitelist_import_many_ENG_24.csv'
file_path = '/home/johan/csv-pdf/data/csv/eventservice-events-db.csv'
imagePath = '/home/johan/csv-pdf/data/csv/image/people.jpg'

df = pd.read_csv(file_path)
df_white = pd.read_csv(file_path_whitelist)

# Load whitelist into a set for faster membership checking
whitelist_set = set(df_white.iloc[:, 1])

class PDF(FPDF):

    def __init__(self):
        super().__init__(orientation='L', unit='mm', format='A4')

    def header(self):
        self.set_font('Arial', 'B', 12)
        # self.cell(0, 10, 'Your Company Name', 0, 0, 'L')
        # self.cell(0, 10, 'Address Line 1', 0, 0, 'L')
        # self.cell(0, 10, 'Address Line 2', 0, 0, 'L')
        # self.cell(0, 10, 'City, State, ZIP', 0, 0, 'L')
        # self.cell(0, 10, 'Phone: (123) 456-7890', 0, 0, 'L')
        # self.cell(0, 10, 'Email: info@example.com', 0, 0, 'L')
        # self.cell(0, 10, 'Website: www.example.com', 0, 0, 'L')
        # self.cell(0, 10, 'Page ' + str(self.page_no()), 0, 0, 'R')
        # self.ln(15)
        pass

    def footer(self):
        # self.set_y(-15)
        # self.set_font('Arial', 'I', 8)
        # self.cell(0, 10, 'Page ' + str(self.page_no()), 0, 0, 'C')
        pass

def white_list_fxn(whitelist):
    for index, row in df_white.iterrows():
        # print(row[1])
        whitelist.append(row[1])


def is_date_in_range(date_string, start_date, end_date):
    input_date = parser.parse(date_string)
    input_date = input_date.replace(tzinfo=tz.tzutc())  # Set the timezone to UTC
    start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=tz.tzutc())
    end_date = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=tz.tzutc())

    return start_date <= input_date <= end_date


def main():
    # filter date
    # start_range = "2023-07-28"
    # end_range = "2023-08-05"
    start_range = "2023-07-31 00:00:00+00"
    end_range = "2023-08-05 00:00:00+00"
    # end

    #progress bar
    total_iterations = min(200, len(df))

    whitelist = []
    white_list_fxn(whitelist)
    date_row = []
    # for index in tqdm(range(total_iterations), desc="Processing", unit="row"):
    #     row = df.iloc[index]
    for index, row in df.head(100).iterrows():
        # for index, row in df.iterrows():
        id = row[0]
        timestamps = row[5]
        json_obj = json.loads(row[3])
        license_plate = json_obj['metadata']['license_plate']
        whiteListVerify = license_plate in whitelist_set

        #Filter whiteList
        # if whiteListVerify:
        #     continue
        #end

        # timestamp = is_date_in_range(timestamps, start_range, end_range)
        # if timestamp is False:
        #     return
        #
        # else:
        #     timestamp = timestamps
        # timestamp = timestamps
        # |ID | No.Plate| Timestamp |Direction |Whitelist
        # date_row.append([id, license_plate, timestamp, "IN", whiteListVerify])

        if start_range <= timestamps <= end_range:
            timestamp = timestamps
            date_row.append((id, license_plate, timestamp, "IN", whiteListVerify))

    # for index, item in enumerate(date_row):
    #     # print(f"{index}. {item}")
    #     print(item)

    chunk_size = 5
    chunks = [date_row[i:i + chunk_size] for i in range(0, len(date_row), chunk_size)]

    chunkData = []
    for chunk in chunks:
        # print(chunk)
        chunkData.append(chunk)
    print(chunkData)

    # for index,data in enumerate(chunkData):
    #     print(f"{index}. {data}")

    pdf = PDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    col_widths = [55, 20, 35, 25, 25, 35, 35, 35]  #
    collumnHeight = 40

    # table header
    pdf.set_fill_color(200, 220, 255)
    pdf.cell(col_widths[0], 10, 'ID', 1, 0, 'C', 1)
    pdf.cell(col_widths[1], 10, 'No.Plate', 1, 0, 'C', 1)
    pdf.cell(col_widths[2], 10, 'Timestamp', 1, 0, 'C', 1)
    pdf.cell(col_widths[3], 10, 'Direction', 1, 0, 'C', 1)
    pdf.cell(col_widths[4], 10, 'Device IP', 1, 0, 'C', 1)
    pdf.cell(col_widths[5], 10, 'Whitelist', 1, 0, 'C', 1)
    pdf.cell(col_widths[6], 10, 'Image', 1, align='C', fill=1)
    pdf.cell(col_widths[7], 10, 'CropImage', 1, 1, 'C', 1)
    print('Start Building Pdf loading...')

    # for headerName in chunkData:
    # for sublist in chunkData:
    #     deviceId, license_plate, timestamp, direction, whiteListVerify = sublist
    #     try:
    #         print(deviceId)
    #     except :
    #         print(f'error')
        # pdf.set_font("Arial", size=8)
        # direction = "IN"
        # pdf.set_font("Arial", size=8)
        # pdf.cell(col_widths[0], collumnHeight, deviceId, 1, 0, 'C')
        # pdf.cell(col_widths[1], collumnHeight, license_plate, 1, align='C')
        # pdf.cell(col_widths[2], collumnHeight, timestamp, 1, align='C')
        # pdf.cell(col_widths[3], collumnHeight, direction, 1, align='C')
        # pdf.cell(col_widths[4], collumnHeight, "camera_ip", 1, align='C')
        # pdf.cell(col_widths[5], collumnHeight, str(whiteListVerify).upper(), 1, align='C')
    # printed nested
    # for sublist in chunkData:
    #     for item in sublist:
    #         # print(f"ID: {item[0]}, Plate: {item[1]}, Timestamp: {item[2]}, Status: {item[3]}, Valid: {item[4]}")
    #         pdf.set_font("Arial", size=8)
    #         direction = "IN"
    #         pdf.set_font("Arial", size=8)
    #         pdf.cell(col_widths[0], collumnHeight, item[0], 1, 0, 'C')
    #         pdf.cell(col_widths[1], collumnHeight, item[1], 1, align='C')
    #         pdf.cell(col_widths[2], collumnHeight, item[2], 1, align='C')
    #         pdf.cell(col_widths[3], collumnHeight, direction, 1, align='C')
    #         pdf.cell(col_widths[4], collumnHeight, "camera_ip", 1, align='C')
    #         pdf.cell(col_widths[5], collumnHeight, str(item[4]), 1, align='C')

    # for sublist in chunkData:
    #     # print(len(sublist))
    #     for data in range(len(sublist)):
    #         print(data)
    #     deviceId, license_plate, timestamp, direction, whiteListVerify = sublist
    #
    #     # Convert values to strings
    #     deviceId = str(deviceId)
    #     license_plate = str(license_plate)
    #     timestamp = str(timestamp)
    #     direction = str(direction)
    #     whiteListVerify = str(whiteListVerify)
    #
    #     pdf.cell(col_widths[0], collumnHeight, deviceId, 1, 0, 'C')
    #     pdf.cell(col_widths[1], collumnHeight, license_plate, 1, align='C')
    #     pdf.cell(col_widths[2], collumnHeight, timestamp, 1, align='C')
    #     pdf.cell(col_widths[3], collumnHeight, direction, 1, align='C')
    #     pdf.cell(col_widths[4], collumnHeight, 'camera_ip', 1, align='C')
    #     pdf.cell(col_widths[5], collumnHeight, whiteListVerify.upper(), 1, align='C')
    #
    #     pdf.ln()  # Move to the next line for the next sublist
    #
    # Create the output directory if doesn't exist
    # output_dir = '/home/johan/csv-pdf/data/NoChunk/'
    # os.makedirs(output_dir, exist_ok=True)
    # # Save PDF
    # pdf_output_path = f'/home/johan/csv-pdf/data/NoChunk/report_license_new.pdf'
    # pdf.output(pdf_output_path)

main()
