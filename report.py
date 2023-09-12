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
from tqdm import tqdm
import platform


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


class PDFGenerator:
    def __init__(self, filter=False, whitelist=None):
        self.current_directory = os.getcwd()
        self.lprdata = os.path.join(self.current_directory, r'data\lpr-database.csv')
        self.whiteList = os.path.join(self.current_directory, r'data\whitelist_import_many_ENG_24 .csv')
        self.df = pd.read_csv(self.lprdata)
        self.df_white = pd.read_csv(self.whiteList)
        self.filter = filter
        if self.filter:
            self.rows_per_chunk = 300  # Increase rows_per_chunk when filtering
        else:
            self.rows_per_chunk = 15
        self.num_threads = 4
        self.total_rows = len(self.df)
        self.num_chunks = (self.total_rows + self.rows_per_chunk - 1) // self.rows_per_chunk

        # Determine the appropriate path separator based on the operating system
        if platform.system() == 'Windows':
            self.path_separator = '\\'
        else:
            self.path_separator = '/'

        # Specify the output directory relative to the current script
        self.output_dir = f'.{self.path_separator}NoChunk{self.path_separator}'
        os.makedirs(self.output_dir, exist_ok=True)

        if whitelist is not None:
            self.whitelist = whitelist
        else:
            self.whitelist = self.get_whitelist()

    def get_whitelist(self):
        whitelist = []
        for index, row in self.df_white.iterrows():
            whitelist.append(row.iloc[1])
        return whitelist

    def is_date_in_range(self, date_string, start_date, end_date):
        input_date = parser.parse(date_string)
        input_date = input_date.replace(tzinfo=tz.tzutc())
        start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=tz.tzutc())
        end_date = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=tz.tzutc())
        return start_date <= input_date <= end_date

    def white_list_fxn(self, whitelist):
        for index, row in self.df_white.iterrows():
            whitelist.append(row.iloc[1])

    def generate_pdf_chunk(self, chunk_idx):
        whitelist = []
        start_idx = chunk_idx * self.rows_per_chunk
        end_idx = min((chunk_idx + 1) * self.rows_per_chunk, self.total_rows)

        self.white_list_fxn(whitelist)

        pdf = PDF()
        pdf.add_page()
        pdf.set_font("Arial", size=12)

        col_widths = [55, 20, 35, 25, 25, 35, 35, 35]
        collumnHeight = 40

        pdf.set_fill_color(200, 220, 255)
        pdf.cell(col_widths[0], 10, 'ID', 1, 0, 'C', 1)
        pdf.cell(col_widths[1], 10, 'No.Plate', 1, 0, 'C', 1)
        pdf.cell(col_widths[2], 10, 'Timestamp', 1, 0, 'C', 1)
        pdf.cell(col_widths[3], 10, 'Direction', 1, 0, 'C', 1)
        pdf.cell(col_widths[4], 10, 'Device IP', 1, 0, 'C', 1)
        pdf.cell(col_widths[5], 10, 'Whitelist', 1, 0, 'C', 1)
        pdf.cell(col_widths[6], 10, 'Image', 1, align='C', fill=1)
        pdf.cell(col_widths[7], 10, 'CropImage', 1, 1, 'C', 1)

        for index in range(start_idx, end_idx):
            row = self.df.iloc[index]
            resImage = json.loads(row.iloc[4])
            json_data = row.iloc[3]
            base64_data = (resImage['image'])

            decoded_data = base64.b64decode(base64_data)
            pil_image = Image.open(io.BytesIO(decoded_data))
            temp_image_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
            pil_image.save(temp_image_file, format="PNG")
            temp_image_file.close()

            base64_data_crop_image = (resImage['cropped_image'])
            decoded_data_cropped = base64.b64decode(base64_data_crop_image)
            pil_image_crop = Image.open(io.BytesIO(decoded_data_cropped))
            temp_image_file_crop = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
            pil_image_crop.save(temp_image_file_crop, format="PNG")
            temp_image_file_crop.close()

            try:
                json_obj = json.loads(json_data)
                deviceId = row.iloc[0]
                camera_ip = json_obj['metadata']['camera_ip']
                license_plate = json_obj['metadata']['license_plate']
                whiteListVerify = license_plate in self.whitelist
                timestamp = row.iloc[5]

                if self.filter and not whiteListVerify:
                    continue

                pdf.set_font("Arial", size=8)
                direction = "IN"
                pdf.set_font("Arial", size=8)
                pdf.cell(col_widths[0], collumnHeight, deviceId, 1, 0, 'C')
                pdf.cell(col_widths[1], collumnHeight, license_plate, 1, align='C')
                pdf.cell(col_widths[2], collumnHeight, timestamp, 1, align='C')
                pdf.cell(col_widths[3], collumnHeight, direction, 1, align='C')
                pdf.cell(col_widths[4], collumnHeight, camera_ip, 1, align='C')
                pdf.cell(col_widths[5], collumnHeight, str(whiteListVerify).upper(), 1, align='C')
                pdf.image(temp_image_file.name, x=pdf.get_x(), y=pdf.get_y(), w=col_widths[6], h=collumnHeight)
                pdf.cell(col_widths[6], collumnHeight, "", 1, align='C')
                pdf.image(temp_image_file_crop.name, x=pdf.get_x(), y=pdf.get_y(), w=col_widths[7], h=collumnHeight)
                pdf.cell(col_widths[7], collumnHeight, "", border=1, ln=1, align='C')
            except (json.JSONDecodeError, KeyError) as e:
                pdf.cell(sum(col_widths), 10, f"Error processing row {index}: {e}", 1, 1)

        pdf_output_path = f'{self.output_dir}report_license_{chunk_idx + 1}.pdf'
        pdf.output(pdf_output_path)

    def generate_pdfs(self):
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            list(tqdm(executor.map(self.generate_pdf_chunk, range(self.num_chunks)), total=self.num_chunks,
                      desc="Generating PDFs"))
        print("PDF report generated successfully.")


pdf_generator = PDFGenerator(filter=True,whitelist=["WUV9888","WYW7737"])
# pdf_generator = PDFGenerator(whitelist=["WUV9888","WYW7737"])
pdf_generator.generate_pdfs()
