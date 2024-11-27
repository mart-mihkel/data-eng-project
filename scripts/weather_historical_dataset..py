import os
import pandas as pd


def process_weather_data(input_path, output_path):
    """
    Process an Excel file to add a city column, clean the data, and translate headers to English.

    Parameters:
        input_path (str): Path to the input Excel file.
        output_path (str): Path to save the updated Excel file.

    Returns:
        None
    """
    # Load the Excel file
    data = pd.ExcelFile(input_path)

    # Load the first sheet into a DataFrame
    df = data.parse(sheet_name=0, header=None)  # Load without assuming headers

    # Step 1: Drop the top blank row
    df = df.dropna(how="all")  # Drop rows where all values are NaN

    # Step 2: Extract the city name from the second row, fifth column (index 4)
    city_name = df.iloc[0, 4]  # Extract city name

    # Step 3: Set the second row as the column headers
    df.columns = df.iloc[1]  # Use the second row as headers
    df = df[2:]  # Remove the first two rows

    # Step 4: Add a "City" column and populate it with the extracted city name
    df["City"] = city_name

    # Step 5: Translate headers from Estonian to English
    header_translation = {
        "Aasta": "Year",
        "Kuu": "Month",
        "Päev": "Day",
        "Kell (UTC)": "Time (UTC)",
        "Õhutemperatuur °C": "Air Temperature (°C)",
        "Tunni miinimum õhutemperatuur °C": "Hourly Minimum Air Temperature (°C)",
        "Tunni maksimum õhutemperatuur °C": "Hourly Maximum Air Temperature (°C)",
        "10 minuti keskmine tuule suund °": "10 Min Average Wind Direction (°)",
        "10 minuti keskmine tuule kiirus m/s": "10 Min Average Wind Speed (m/s)",
        "Tunni maksimum tuule kiirus m/s": "Hourly Maximum Wind Speed (m/s)",
        "Õhurõhk merepinna kõrgusel hPa": "Air Pressure at Sea Level (hPa)",
        "Tunni sademete summa mm": "Hourly Precipitation Total (mm)",
        "Õhurõhk jaama kõrgusel hPa": "Air Pressure at Station Height (hPa)",
        "Suhteline õhuniiskus %": "Relative Humidity (%)",
        "City": "City",
        "Tunni keskmine summaarne kiirgus W/m²": "Hourly Average Total Radiation W/m²"

    }

    # Translate headers where possible
    df.rename(columns=lambda x: header_translation.get(x, x), inplace=True)

    # Step 6: Reset the index for a clean output
    df.reset_index(drop=True, inplace=True)

    # Save the updated DataFrame to a new Excel file
    df.to_excel(output_path, index=False)
    print(f"Processed file saved as: {output_path}")


def main():
    """
    Main function to process all Excel files in the dataset folder.
    """
    # Define the input and output folder paths
    input_folder = "dataset"
    output_folder = "output"
    os.makedirs(output_folder, exist_ok=True)  # Ensure output directory exists

    # Loop through all Excel files in the dataset folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".xlsx"):  # Process only Excel files
            input_path = os.path.join(input_folder, filename)
            output_filename = filename.replace(".xlsx", "_cleaned.xlsx")
            output_path = os.path.join(output_folder, output_filename)

            # Process the current Excel file
            process_weather_data(input_path, output_path)


if __name__ == "__main__":
    main()
