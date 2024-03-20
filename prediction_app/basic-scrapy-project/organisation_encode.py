import json
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

def normalize_experience_data(df):
    """
    Normalizes experience data in a DataFrame.

    This function takes the DataFrame containing the profile data read from json file.
    It extracts relevant information from the JSON-encoded strings and creates a new DataFrame
    with normalized columns for profile name, organization profile, position,
    organization, start time, end time, and duration.

    Parameters:
    df: The DataFrame containing the profile data.

    Returns:
    DataFrame: A new DataFrame with normalized columns for profile name, organization profile,
    position, organization, start time, end time, and duration.
    """
    df['experience'] = df['experience'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)

    # Create empty lists to store extracted data
    profile_names = []
    organization_profiles = []
    positions = []
    organisation = []
    start_times = []
    end_times = []
    durations = []

    for idx, row in df.iterrows():
        name = row['profile']
        experience_data = row['experience']

        for exp in experience_data:
            profile_names.append(name)
            organization_profiles.append(exp.get("organisation_profile", ""))
            positions.append(exp.get("position", ""))
            organisation.append(exp.get("organisation", ""))
            start_times.append(exp.get("start_time", ""))
            end_times.append(exp.get("end_time", ""))
            durations.append(exp.get("duration", ""))

    df = pd.DataFrame({
        'profile_name': profile_names,
        'organization_profile': organization_profiles,
        'position': positions,
        'organisation': organisation,
        'start_time': start_times,
        'end_time': end_times,
        'duration': durations
    })

    return df

def transform_organisation(df):
    """
    Check for all the companies in MAS group and convert them all to MAS Holdings.

    Parameters:
    df: DataFrame returned from normalize_experience_data() function.

    Returns:
    df: A new DataFrame with the 'organisation' column transformed according to the specified conditions.
    """
    df['organisation'] = df['organisation'].str.lower()

    # convert all the companies starting from characters mas to mas holdings.
    mask = df['organisation'].str[:3] == 'mas'
    df.loc[mask, 'organisation'] = 'mas holdings'

    # List of child organizations and the parent organization
    child_organizations = ['mas intimates', 'mas kreeda', 'mas active', 'linea aqua', 'mas Linea Aqua', 'bodyline', 'mas legato',
                           'silueta - technologies by mas', 'twinery - innovations by mas', 'noyon lanka pvt ltd', 'mas matrix',
                           'hellmann mas supply chain', 'silueta', 'twinery', 'noyon',
                          ]

    parent_organization = 'mas holdings'

    # Update the 'organisation' column for child organizations
    for child_org in child_organizations:
        df.loc[df['organisation'].str.contains(child_org, case=False, na=False), 'organisation'] = parent_organization

    return df

def merge_and_select_columns(df):
    """
    This function reads company details data from a JSON file and merges it with
    the profile DataFrame based on a common column ('organisation').
    It then selects specific columns from the merged DataFrame and creates a new DataFrame with
    the selected columns.

    Parameters:
    df: Input DataFrame.

    Returns:
    DataFrame: A new DataFrame containing selected columns after merging.
    """
    # Read company details data
    file_path2 = r'data/filtered_companies_combined.json'
    df2 = pd.read_json(file_path2)

    # Convert 'organisation' column to lowercase
    df2['name'] = df2['name'].str.lower()

    # Merge the DataFrames based on the lowercase 'organisation' column
    df_merged = pd.merge(df, df2, left_on=df.columns[3], right_on=df2.columns[0], how='left')

    # Select necessary columns
    selected_columns = ['profile_name', 'position', 'start_time', 'end_time', 'organisation', 'duration', 'industry', 'size',
                        'founded', 'headquaters']

    # Create a new DataFrame with selected columns
    df = df_merged[selected_columns]

    return df

def clean_and_map_headquarters(df):
    """
    Check if the headquater is Sri Lanka or not. Then convert the headquater column to a binary column.

    Parameters:
    df: The DataFrame returned from merge_and_select_columns.

    Returns:
    df: A new DataFrame with cleaned and encoded headquarters data.
    """
    # Cleaning headquaters (Removing numbers and strings after comma)
    def clean_headquarters(value, part):
        if pd.notna(value):
            parts = value.split(',')
            if len(parts) > part:
                return ''.join(filter(str.isalpha, parts[part]))
        return value

    df['headquaters_part1'] = df['headquaters'].apply(lambda x: clean_headquarters(x, 0))
    df['headquaters_part2'] = df['headquaters'].apply(lambda x: clean_headquarters(x, 1))

    # Defining country mapping function
    def get_country_from_city(city_name):
        geolocator = Nominatim(user_agent="city-to-country")

        try:
            location = geolocator.geocode(city_name, timeout=10)  # Adjust the timeout value as needed
            if location:
                country_name = location.address.split(",")[-1].strip()
                if country_name == "ශ්‍රී ලංකාව இலங்கை":
                    country_name = "Sri Lanka"
                elif country_name == "Italia":
                    country_name = "Unknown"
                return country_name
        except GeocoderTimedOut:
            print("Geocoding service timed out. Retrying...")
            return get_country_from_city(city_name)

        return 'unidentified'

    df['headquaters_part1'] = df['headquaters_part1'].apply(lambda city: get_country_from_city(city))
    df['headquaters_part2'] = df['headquaters_part2'].apply(lambda city: get_country_from_city(city))

    # Define function to check if headquarters is in Sri Lanka
    def is_in_sri_lanka(row):
        if 'Sri Lanka' in row['headquaters_part1'] or 'Sri Lanka' in row['headquaters_part2']:
            return 1
        else:
            return 0

    df['headquaters'] = df.apply(is_in_sri_lanka, axis=1)

    columns_to_drop = ['headquaters_part1', 'headquaters_part2']
    df.drop(columns=columns_to_drop, inplace=True)

    return df

def convert_duration_to_months(df):
    """
    This function converts the 'duration' column in the input DataFrame from various formats
    (e.g., '2 years', '6 months') to a unified format of months. It then updates the 'duration'
    column in the DataFrame with the converted values.

    Parameters:
    df: The df returned from the clean_and_map_headquarters() function.

    Returns:
    df: A new DataFrame with the 'duration' column converted to months.
    """
    # Define a function to convert duration to month
    def duration_to_months(duration):
        # Check if the duration value is a string
        if isinstance(duration, str):
            if "less than a year" in duration.lower():
                return 0

            # Separate a string such as '2 years' into parts such as 2 and years
            total_months = 0
            parts = duration.split()

            # Loop through every item. Since one item has two parts, the loop jumps every two items
            for i in range(0, len(parts), 2):
                # Numeric part is converted to int and stored in the variable value.
                value = int(parts[i])
                # Second part is converted to lowercase and stored in the variable unit
                unit = parts[i + 1].lower()

                # Check the unit and add the relevant number of months according to years or months
                if 'year' in unit:
                    total_months += value * 12
                elif 'month' in unit:
                    total_months += value

            return total_months
        else:
            # If it's already an integer, assume it's in months
            return duration

    # Convert durations to months by calling the function on the 'duration' column
    df['duration'] = df['duration'].apply(duration_to_months)

    return df

def transform_date_columns(df):
    """
    This function transforms date columns in a DataFrame.

    Parameters:
    df: The DataFrame returned from the convert_duration_to_months() function.

    Returns:
    df: A new DataFrame with transformed date columns and calculated recency values.
    """
    # Define the reference date
    reference_date = pd.to_datetime('2023-01-01')

    def convert_month_year_to_date(value):
        try:
            if isinstance(value, str):
                if len(value) == 4:
                    # Assume it's in "yyyy" format
                    date = pd.to_datetime(value, format='%Y')
                elif len(value) > 4:
                    # Assume it's in "Mon yyyy" format
                    date = pd.to_datetime(value, format='%b %Y')
                else:
                    # If the length doesn't match either format, return None
                    date = None
            else:
                # If it's not a string, assume it's already a datetime object
                date = value

            return date
        except ValueError:
            return None

    # Convert 'start_time'
    df['start_time'] = df['start_time'].apply(convert_month_year_to_date)

    # Set 'end_time' to January 1st, 2023, when marked as "present"
    df['end_time'] = df.apply(
        lambda row: pd.to_datetime('2023-01-01') if (row['end_time'] == 'present' or pd.to_datetime(row['end_time']) > pd.to_datetime('2023-01-01')) else convert_month_year_to_date(row['end_time']),
        axis=1
    )

    # Filter out rows that started in January 2023
    df = df[df['start_time'].dt.year != 2023]

    # Calculate the 'start_recency_months'
    df['start_recency_months'] = (reference_date - df['start_time']).dt.days // 30

    # Calculate the 'end_recency_months'
    df['end_recency_months'] = (reference_date - df['end_time']).dt.days // 30

    return df

def add_apparel_industry_column(df):
    """
    This function adds an 'apparel_industry' column to the DataFrame.

    Parameters:
    df: The DataFrame returned from the transform_date_columns() function.

    Returns:
    df: A new DataFrame with an additional 'apparel_industry' column indicating whether each entry belongs to the apparel industry.
    """
    df['apparel_industry'] = df['industry'].apply(lambda x: 1 if 'apparel' in str(x).lower() else 0)

    return df

def create_company_size_column(df):
    """
    This function creates a 'company_size' column in the DataFrame based on the 'size' column.

    Parameters:
    df: The DataFrame returned from the add_apparel_industry_column() function.

    Returns:
    df: A new DataFrame with a 'company_size' column based on the 'size' column.
    """
    # Define the size mapping
    size_mapping = {
        '1,000 - employees': 1,
        '1,001-5,000 employees': 2,
        '5,001-10,000 employees': 3,
        '10,001+ employees': 4
    }

    # Create the 'company_size' column based on the 'size'
    df['company_size'] = df['size'].map(size_mapping)

    return df

def calculate_company_age_column(df):
    """
    This function calculates the 'company_age_years' column in the DataFrame based on the 'founded' column.

    Parameters:
    df: The DataFrame returned from the create_company_size_column() function.

    Returns:
    df: A new DataFrame with a 'company_age_years' column calculated based on the 'founded' column.
    """
    # Convert 'founded' to numeric
    df['founded'] = pd.to_numeric(df['founded'])

    # Calculate the 'company_age_years' column
    df['company_age_years'] = df['founded'].apply(lambda x: 0 if x == 0 else 2023 - x)

    return df

def calculate_job_level(df):
    """
    This function calculates the job level of each position in the DataFrame based on grade and designation mappings.

    Parameters:
    df: DataFrame
        The DataFrame returned from the calculate_company_age_column() function.

    Returns:
    df: DataFrame
        A new DataFrame with a 'job_level' column indicating the job level of each position.
    """
    # Read grade_mapping and designation_mapping data frames
    grade_mapping_df = pd.read_excel("grade_mapping.xlsx")
    designation_mapping_df = pd.read_excel("designation_mapping.xlsx")

    # Convert the relevant columns to lowercase for case-insensitive matching
    grade_mapping_df['Grade'] = grade_mapping_df['Grade'].str.lower()
    designation_mapping_df['Designation'] = designation_mapping_df['Designation'].str.lower()
    df['position'] = df['position'].str.lower()

    # Sort data frames by the length of strings in descending order
    grade_mapping_df = grade_mapping_df.sort_values(by='Grade', key=lambda x: x.str.len(), ascending=False)
    designation_mapping_df = designation_mapping_df.sort_values(by='Designation', key=lambda x: x.str.len(), ascending=False)

    # Define a function to map the job position to the corresponding job level
    def get_job_level(position):
        if "senior" in position and "manager" not in position and "director" not in position:
            return 4 
        if "assistant" in position and "manager" not in position and "executive" not in position:
            return 1 
        # Check if the position is in grade_mapping_df
        for grade in grade_mapping_df['Grade']:
            if grade in position:
                return grade_mapping_df.loc[grade_mapping_df['Grade'] == grade, 'Level'].values[0]

        # Check if the position is in designation_mapping_df 
        for designation in designation_mapping_df['Designation']:
            if designation in position:
                return designation_mapping_df.loc[designation_mapping_df['Designation'] == designation, 'Level'].values[0]

        # If no match is found, return 0
        return 0

    # Apply the get_job_level function and create a new "Job Level" column
    df['job_level'] = df['position'].apply(get_job_level)

    df = df[df['job_level'] != 0]

    return df

def calculate_company_change(df):
    """
    This function calculates changes in company affiliation for each profile in the DataFrame.

    Parameters:
    df: DataFrame
        The DataFrame returned from the calculate_job_level() function.

    Returns:
    df: DataFrame
        A new DataFrame with a 'company_change' column indicating changes in company affiliation for each profile.
    """
    # Sort the dataframe vy profile and start time
    df.sort_values(by=['profile_name', 'start_time'], ascending=[False, False], inplace=True)

    # Reset indexes
    df = df.reset_index(drop=True)

    # Initialize an empty list to store the values for the new "company_change" column
    company_change = []

    # Initialize a variable to keep track of the current profile
    current_profile = None
    previous_organization = None

    # Iterate over the rows of the DataFrame in reverse order
    for index in reversed(df.index):
        row = df.loc[index]
        if row['profile_name'] != current_profile:
            # If the profile has changed, set the company_change value to -1
            company_change.insert(0, -1)
            current_profile = row['profile_name']
            previous_organization = row['organisation']
        else:
            # Check if the organization has changed compared to the next row
            if row['organisation'] != previous_organization:
                company_change.insert(0, 1)  # Organization changed
            else:
                if previous_organization is None:
                    company_change.insert(0, -1)  # First organization in profile
                else:
                    company_change.insert(0, 0)  # Organization did not change
            previous_organization = row['organisation']

    # Add the "company_change" column to the DataFrame
    df['company_change'] = company_change

    return df

def calculate_cumulative_company_changes(df):
    """
    This function calculates the cumulative changes in company affiliation for each profile in the DataFrame.

    Parameters:
    df: DataFrame
        The DataFrame returned from the calculate_company_change() function.

    Returns:
    df: DataFrame
        A new DataFrame with a 'cum_no_of_companies' column indicating the cumulative changes in company affiliation for each profile.
    """
    cumulative_counts = {}  # Initialize a dictionary to store the cumulative counts for each profile
    previous_organizations = {}     # Initialize a dictionary to store the set of previous organizations for each profile
    current_profile = None      # Initialize a variable to keep track of the current profile
    cumulative_count = 0    # Initialize a variable to store the cumulative count
    
    # Iterate over the rows of the DataFrame in reverse order
    for index in reversed(df.index):
        row = df.loc[index]
        if row['company_change'] == -1:
            # If the company change is -1, reset the cumulative count to 1
            cumulative_count = 1
            current_profile = row['profile_name']
            previous_organizations[current_profile] = set()
        else:
            if row['profile_name'] != current_profile:
                # If the profile has changed, reset the cumulative count to 1
                cumulative_count = 1
                current_profile = row['profile_name']
                previous_organizations[current_profile] = set()
            
            # Check if the current organization has not occurred in previous rows of the current profile
            if row['company_change'] == 1 and row['organisation'] not in previous_organizations[current_profile]:
                cumulative_count += 1
                
        # Store the cumulative count for the current row in the dictionary
        cumulative_counts[index] = cumulative_count
        
        # Add the current organization to the set of previous organizations for the current profile
        previous_organizations[current_profile].add(row['organisation'])
    
    # Create a list of cumulative counts based on the DataFrame rows
    cumulative_count_list = [cumulative_counts[index] for index in df.index]
    
    # Add the cumulative count as a new column in the DataFrame
    df['cum_no_of_companies'] = cumulative_count_list

    return df

def calculate_level_up(df):
    """
    This function calculates the level up changes for each profile in the DataFrame.
    The level is the difference between the job level of the current row and the previous row of each profile_name.

    Parameters:
    df: DataFrame
        The DataFrame returned from the calculate_cumulative_company_changes() function.

    Returns:
    df: DataFrame
        A new DataFrame with a 'level_up' column indicating level up changes for each profile.
    """
    # Initialize an empty list to store the values for the new "company_change" column
    level_ups = []

    # Initialize a variable to keep track of the current profile
    current_profile = None
    previous_job_level = None

    # Iterate over the rows of the DataFrame in reverse order
    for index in reversed(df.index):
        row = df.loc[index]
        if row['profile_name'] != current_profile:
            # If the profile has changed, set the company_change value to -1
            level_ups.insert(0, -1)
            current_profile = row['profile_name']
            previous_job_level = row['job_level']  # Reset the previous_organization
        else:
            # Check if the organization has changed compared to the next row
            # next_row = new_selected_df.loc[new_selected_df.index[new_selected_df.index.get_loc(index) - 1]]
            if row['job_level'] != 0:
                level_up = row['job_level'] - previous_job_level
                level_ups.insert(0, level_up)  # Organization changed
                previous_job_level = row['job_level']
            else:
                level_up = -5
                level_ups.insert(0, level_up)
                continue 
    
    df['level_up'] = level_ups

    return df

def calculate_lateral_movements(df):
    """
    This function calculates the lateral movements for each profile in the DataFrame.
    Lateral movements are defined as movements within the same company without a change in job level.

    Parameters:
    df: The DataFrame returned from the calculate_level_up() function, containing information about job level changes.

    Returns:
    df: A new DataFrame with a 'lateral_movements' column indicating lateral movements for each profile.
        The 'lateral_movements' column contains binary values:
            - 1: indicates a lateral movement within the same company.
            - 0: indicates no lateral movement.
            - -1: indicates a change in profile.
    """
    # Initialize an empty list to store the values for the new "company_change" column
    lateral_movements = []

    # Initialize a variable to keep track of the current profile
    current_profile = None

    # Iterate over the rows of the DataFrame in reverse order
    for index in reversed(df.index):
        row = df.loc[index]
        if row['profile_name'] != current_profile:
            # If the profile has changed, set the company_change value to -1
            lateral_movements.insert(0, -1)
            current_profile = row['profile_name']
        else:
            # Check if the organization has changed compared to the next row
            # next_row = new_selected_df.loc[new_selected_df.index[new_selected_df.index.get_loc(index) - 1]]
            if row['company_change'] == 0 and row['level_up'] == 0:
                lateral_movements.insert(0, 1)  # Organization changed
            else:
                lateral_movements.insert(0, 0)
    
    df['lateral_movements'] = lateral_movements

    return df

def transform_and_reorder_columns(df):
    """
    This function transforms and reorders columns in the DataFrame.

    Parameters:
    df: The DataFrame returned from the calculate_lateral_movements() function.

    Returns:
    df: A new DataFrame with transformed and reordered columns according to specified order.
    """
    # Drop unnecessary columns
    columns_to_drop = ['position', 'start_time', 'end_time', 'organisation', 'industry', 'size', 'founded']
    df.drop(columns=columns_to_drop, inplace=True)

    # Reorder the columns in the DataFrame
    desired_order = ['profile_name', 'duration', 'start_recency_months', 'end_recency_months', 'job_level', 'company_change',
                     'cum_no_of_companies', 'level_up', 'lateral_movements', 'apparel_industry', 'company_size',
                     'company_age_years', 'headquaters']
    df = df[desired_order]

    return df

def transform_data(input_file_path):
    """
    This function orchestrates the transformation of input data.

    It reads the data from the given input file path, performs a series of transformations on it,
    and returns the resulting DataFrame.

    Parameters:
    input_file_path: The file path to the input JSON data.

    Returns:
    df: The transformed DataFrame after applying all necessary transformations.
    """
    df = pd.read_json(input_file_path)
    df = normalize_experience_data(df)
    df = transform_organisation(df)
    df = merge_and_select_columns(df)
    df = clean_and_map_headquarters(df)
    df = convert_duration_to_months(df)
    df = transform_date_columns(df)
    df = add_apparel_industry_column(df)
    df = create_company_size_column(df)
    df = calculate_company_age_column(df)
    df = calculate_job_level(df)
    df = calculate_company_change(df)
    df = calculate_cumulative_company_changes(df)
    df = calculate_level_up(df)
    df = calculate_lateral_movements(df)
    df = transform_and_reorder_columns(df)

    return df

# Example Usage:
input_file_path = 'data\\scraped_people_data\\profile.json'
experience_df = transform_data(input_file_path)

# Save the transformed DataFrame as a new CSV file if needed
experience_df.to_csv('encoded_data\\experience_encoded.csv', index=False)
