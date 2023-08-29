import json
import os
from datetime import datetime
from dateutil import parser
from dateutil import tz

#for pdf
from fpdf import FPDF

from tqdm import tqdm  # progress bar
import pandas as pd

#multithread
from concurrent.futures import ThreadPoolExecutor


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

    chunk_size = 20
    chunks = [date_row[i:i + chunk_size] for i in range(0, len(date_row), chunk_size)]
    num_chunks = (len(df) + chunk_size - 1)
    chunkData = []
    for chunk in chunks:
        chunkData.append(chunk)
    pdf_generator(chunkData)
    num_threads = 4
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        list(tqdm(executor.map(pdf_generator, chunkData), total=num_chunks, desc="Generating PDFs"))
    print("PDF report generated successfully.")


def pdf_generator(chunkData):
    pdf_number = 1
    for page_data in chunkData:
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
        for entry in page_data:
            deviceId, license_plate, timestamp, direction, whiteListVerify = entry
            pdf.set_font("Arial", size=8)
            direction = "IN"
            pdf.set_font("Arial", size=8)
            pdf.cell(col_widths[0], collumnHeight, deviceId, 1, 0, 'C')
            pdf.cell(col_widths[1], collumnHeight, license_plate, 1, align='C')
            pdf.cell(col_widths[2], collumnHeight, timestamp, 1, align='C')
            pdf.cell(col_widths[3], collumnHeight, direction, 1, align='C')
            pdf.cell(col_widths[4], collumnHeight, "camera_ip", 1, align='C')
            pdf.cell(col_widths[5], collumnHeight, str(whiteListVerify).upper(), 1, align='C')
            # pdf.multi_cell(col_widths[5], collumnHeight, resImage['image'], 1, align='C', fill=True)
            image_paths = '/home/johan/csv-pdf/data/csv/image/person.jpg'
            # pill_images = '/home/johan/csv-pdf/data/csv/image/temp_image.png'
            # pill_images = f'/home/johan/csv-pdf/data/csv/imagelpr/temp_image{index}.png'
            # pdf.image(temp_image_file.name, x=pdf.get_x(), y=pdf.get_y(), w=col_widths[6], h=collumnHeight)
            pdf.cell(col_widths[6], collumnHeight, "", 1, align='C')  # Add border parameter here
            # # pdf.image(pill_images, x=pdf.get_x(), y=pdf.get_y(), w=col_widths[7], h=collumnHeight)
            # pdf.image(temp_image_file_crop.name, x=pdf.get_x(), y=pdf.get_y(), w=col_widths[7], h=collumnHeight)
            pdf.cell(col_widths[7], collumnHeight, "", border=1, ln=1, align='C')

        pdf.output(f"output_{pdf_number}.pdf")
        pdf_number += 1


main()
