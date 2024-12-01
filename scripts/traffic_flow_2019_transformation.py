import pandas as pd
import os

# Translation dictionary for the headers
translations = {
    "Mnt nr": "Road No",
    "Maantee nimetus": "Road Name",
    "Algus m": "Start (m)",
    "Lõpp m": "End (m)",
    "Pikkus m": "Length (m)",
    "AKÖL autot/ööp": "AADT vehicles/day",
    "SAPA %": "SAPA %",
    "VAAB %": "VAAB %",
    "AR %": "AR %",
    "SAPA autot/ööp": "SAPA vehicles/day",
    "VAAB autot/ööp": "VAAB vehicles/day",
    "AR autot/ööp": "AR vehicles/day",
    "Loenduse aasta": "Survey Year",
    "Maakond": "County",
    "Regioon": "Region"
}


# Function to clean and normalize column names
def clean_column_name(col):
    # Remove leading/trailing spaces, extra internal spaces, and newlines
    return " ".join(col.split()).replace("\xa0", " ").strip()


# Process a specific file and auto-detect the first sheet
def process_specific_file(input_file, output_file):
    # Read the first sheet from the Excel file
    df = pd.read_excel(input_file)

    # Normalize column headers: remove extra spaces, newlines, and special characters
    cleaned_headers = [clean_column_name(col) for col in df.columns]
    df.columns = cleaned_headers

    # Translate headers using the dictionary
    translated_headers = [translations.get(col, col) for col in df.columns]
    df.columns = translated_headers

    # Save the cleaned DataFrame to a new Excel file
    df.to_excel(output_file, index=False)
    print(f"Processed and saved: {output_file}")


if __name__ == "__main__":
    # Specific input and output file paths
    input_file = "traffic_flow_dataset/7_lisa_5-7_sagedused_2019_v2_2019.xlsx"
    output_file = "traffic_flow_output/7_lisa_5-7_sagedused_2019_v2_2019_translated.xlsx"

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Process the specific file
    process_specific_file(input_file, output_file)

    print("File processed successfully.")
