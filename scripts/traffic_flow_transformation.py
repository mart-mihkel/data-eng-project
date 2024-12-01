import os
import pandas as pd

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


def process_file(input_file, output_dir):
    # Read the Excel file into a dictionary of DataFrames
    sheets = pd.read_excel(input_file, sheet_name=None)

    # Combine all sheets into one DataFrame
    combined_df = pd.DataFrame()

    for sheet_name, df in sheets.items():
        # Normalize column headers: remove extra spaces, newlines, and special characters
        cleaned_headers = [clean_column_name(col) for col in df.columns]
        df.columns = cleaned_headers

        # Translate headers using the dictionary
        translated_headers = [translations.get(col, col) for col in df.columns]
        df.columns = translated_headers

        # Add a column to track the source sheet
        df["Source Sheet"] = sheet_name

        # Append the current sheet to the combined DataFrame
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    # Define output file path
    output_file_name = os.path.splitext(os.path.basename(input_file))[0] + "_translated.xlsx"
    output_file_path = os.path.join(output_dir, output_file_name)

    # Save the combined DataFrame to a new Excel file
    combined_df.to_excel(output_file_path, index=False, sheet_name="Combined Data")
    print(f"Processed and saved: {output_file_path}")


if __name__ == "__main__":
    # Directories
    input_dir = "traffic_flow_dataset"
    output_dir = "traffic_flow_output"

    os.makedirs(output_dir, exist_ok=True)

    # Loop through all Excel files in the input directory
    for file_name in os.listdir(input_dir):
        input_file_path = os.path.join(input_dir, file_name)
        if os.path.isfile(input_file_path) and file_name.endswith(".xlsx"):
            process_file(input_file_path, output_dir)

    print("All files processed successfully.")
