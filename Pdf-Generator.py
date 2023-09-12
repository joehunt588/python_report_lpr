import json
import tempfile
from datetime import datetime
from dateutil import parser
from dateutil import tz
import pandas as pd
from fpdf import FPDF
import os
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
import io
import base64
from tqdm import tqdm  # Import tqdm
import platform  # Import the platform module to detect the operating system

# file_whitelist = 'data\whitelist_import_many_ENG_24.csv'
# file_path = '/home/johan/csv-pdf/data/csv/eventservice-events-db.csv'
# # Get the current working directory (where main.py is located)
# current_directory = os.path.dirname(__file__)
# # Create the full file path by joining the current directory with the relative file path
# file_path_whitelist = os.path.join(current_directory, file_whitelist)
# # # df = pd.read_csv(file_path)

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

current_directory = os.getcwd()
lprdata = os.path.join(current_directory, r'data\lpr-database.csv')
whiteList = os.path.join(current_directory, r'data\whitelist_import_many_ENG_24 .csv')
df = pd.read_csv(lprdata)
df_white = pd.read_csv(whiteList)

rows_per_chunk =10
num_threads = 4
total_rows = len(df)
num_chunks = (total_rows + rows_per_chunk - 1) // rows_per_chunk
print(f'total row {total_rows}')

def is_date_in_range(date_string, start_date, end_date):
    input_date = parser.parse(date_string)
    input_date = input_date.replace(tzinfo=tz.tzutc())  # Set the timezone to UTC
    start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=tz.tzutc())
    end_date = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=tz.tzutc())

    return start_date <= input_date <= end_date


def generate_pdf_chunk(chunk_idx):
    whitelist = []
    start_idx = chunk_idx * rows_per_chunk
    end_idx = min((chunk_idx + 1) * rows_per_chunk, total_rows)

    white_list_fxn(whitelist)

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
    for index, row in df.iterrows():
    # for index, row in df.head(5).iterrows():
    # for index in range(start_idx, end_idx):
        row = df.iloc[index]
        resImage = json.loads(row.iloc[4])
        json_data = row.iloc[3]
        base64_data = (resImage['image'])
    #     # Decode base64 data
        decoded_data = base64.b64decode(base64_data)
    #
    #     # # Create a PIL image object from the decoded data
        pil_image = Image.open(io.BytesIO(decoded_data))
    #
        temp_image_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        pil_image.save(temp_image_file, format="PNG")
        temp_image_file.close()
    #
        # cropImage
        base64_data_crop_image = (resImage['cropped_image'])
        # Decode base64 data cropImage
        decoded_data_cropped = base64.b64decode(base64_data_crop_image)

        # # Create a PIL image object from the decoded data cropImage
        pil_image_crop = Image.open(io.BytesIO(decoded_data_cropped))

        temp_image_file_crop = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        pil_image_crop.save(temp_image_file_crop, format="PNG")
        temp_image_file_crop.close()
    #
    #     date_to_check = "2023-07-31 03:33:02+00"
    #     start_range = "2023-07-31"
    #     end_range = "2023-08-05"
    #
        try:
            json_obj = json.loads(json_data)
            deviceId = row.iloc[0]
            camera_ip = json_obj['metadata']['camera_ip']
    #         if camera_ip != '192.168.88.94':
    #             return
            license_plate = json_obj['metadata']['license_plate']
            whiteListVerify = license_plate in whitelist
            timestamp = row.iloc[5]
            print(whiteListVerify)
            if not whiteListVerify:
                continue
    #         timestamp = is_date_in_range(row[5], start_range, end_range)
    #         # if timestamp is False:
    #         #     return
    #         #
    #         # else:
    #         #     timestamp = row[5]
    #
            pdf.set_font("Arial", size=8)
            direction = "IN"
            pdf.set_font("Arial", size=8)
            pdf.cell(col_widths[0], collumnHeight, deviceId, 1, 0, 'C')
            pdf.cell(col_widths[1], collumnHeight, license_plate, 1, align='C')
            pdf.cell(col_widths[2], collumnHeight, timestamp, 1, align='C')
            pdf.cell(col_widths[3], collumnHeight, direction, 1, align='C')
            pdf.cell(col_widths[4], collumnHeight, camera_ip, 1, align='C')
            pdf.cell(col_widths[5], collumnHeight, str(whiteListVerify).upper(), 1, align='C')
            # pdf.multi_cell(col_widths[5], collumnHeight, resImage['image'], 1, align='C', fill=True)
            # pill_images = '/home/johan/csv-pdf/data/csv/image/temp_image.png'
            # pill_images = f'/home/johan/csv-pdf/data/csv/imagelpr/temp_image{index}.png'
            pdf.image(temp_image_file.name, x=pdf.get_x(), y=pdf.get_y(), w=col_widths[6], h=collumnHeight)
            pdf.cell(col_widths[6], collumnHeight, "", 1, align='C')  # Add border parameter here
            # pdf.image(pill_images, x=pdf.get_x(), y=pdf.get_y(), w=col_widths[7], h=collumnHeight)
            pdf.image(temp_image_file_crop.name, x=pdf.get_x(), y=pdf.get_y(), w=col_widths[7], h=collumnHeight)
            pdf.cell(col_widths[7], collumnHeight, "", border=1, ln=1, align='C')  # Add border parameter here
        except (json.JSONDecodeError, KeyError) as e:
            pdf.cell(sum(col_widths), 10, f"Error processing row {index}: {e}", 1, 1)

    # # Create the output directory if doesn't exist
    # output_dir = '/home/johan/csv-pdf/data/NoChunk/'
    # os.makedirs(output_dir, exist_ok=True)
    # # Save PDF
    # pdf_output_path = f'/home/johan/csv-pdf/data/NoChunk/report_license_{chunk_idx + 1}.pdf'
    # pdf.output(pdf_output_path)
    # Determine the appropriate path separator based on the operating system
    if platform.system() == 'Windows':
        path_separator = '\\'
    else:
        path_separator = '/'

    # Specify the output directory relative to the current script
    output_dir = f'.{path_separator}NoChunk{path_separator}'

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Save PDF
    pdf_output_path = f'.{path_separator}NoChunk{path_separator}report_license_{chunk_idx + 1}.pdf'
    pdf.output(pdf_output_path)


def white_list_fxn(whitelist):
    for index, row in df_white.iterrows():
        # print(row[1])
        whitelist.append(row.iloc[1])


# Create a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    list(tqdm(executor.map(generate_pdf_chunk, range(num_chunks)), total=num_chunks, desc="Generating PDFs"))
print("PDF report generated successfully.")