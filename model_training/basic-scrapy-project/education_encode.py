import pandas as pd
import datetime
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

def normalize_education_data(df):
    df['education'] = df['education'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    # df['education'] = df['education'].apply(ast.literal_eval)

    # Create empty lists to store extracted data
    profile_names = []
    organization_profiles = []
    organisation = []
    course_details = []
    start_times = []
    end_times = []

    # Iterate through the DataFrame and extract data
    for idx, row in df.iterrows():
        profile = row['profile']
        education_data = row['education']

        for edu in education_data:
            profile_names.append(profile)
            organization_profiles.append(edu.get("organisation_profile", ""))
            organisation.append(edu.get("organisation", ""))
            course_details.append(edu.get("course_details", ""))
            start_times.append(edu.get("start_time", ""))
            end_times.append(edu.get("end_time", ""))

    # Create a DataFrame from the extracted data
    df = pd.DataFrame({
    'profile_name': profile_names,
    'organization_profile': organization_profiles,
    'organisation': organisation,
    'course_details': course_details,
    'start_time': start_times,
    'end_time': end_times
    })
    
    return df

def map_course_level(df):
    # Load course mapping DataFrame
    course_mapping_df = pd.read_excel("course_mapping.xlsx")

    # Convert the relevant columns to lowercase for case-insensitive matching
    course_mapping_df['Course'] = course_mapping_df['Course'].str.lower()
    df['course_details'] = df['course_details'].str.lower()

    # Define a function to map the course details to the corresponding course level
    def get_course_level(course_details):
        for course in course_mapping_df['Course']:
            if course in course_details:
                level = course_mapping_df.loc[course_mapping_df['Course'] == course, 'Level'].values
                if len(level) > 0:
                    return level[0]

        # If no match is found, return 1 as the default level
        return 1

    # Apply the get_course_level function and create a new "course_level" column
    df['course_level'] = df['course_details'].apply(get_course_level)

    return df

def merge_and_select_columns(df):
    # Load institutes data to dataframe
    file_path2 = r'data\filtered_institutes_combined.json'
    df2 = pd.read_json(file_path2)

    # Merge the DataFrames
    merged_df = pd.merge(df, df2, left_on='organisation', right_on='name', how='left')

    # Select necessary columns
    selected_columns_school = ['profile_name', 'organisation', 'start_time', 'end_time', 'course_level', 'founded', 'size', 'headquaters']

    # Create a new DataFrame with selected columns
    df = merged_df[selected_columns_school]

    return df

def transform_date_columns(df):
    current_date = datetime(2023, 1, 31)
    
    # Replace 'present' with the current year
    df['end_time'] = df['end_time'].replace('present', current_date.year)

    # Replace empty strings and NaN values with zeros
    df['start_time'] = df['start_time'].replace('', '0')
    df['end_time'] = df['end_time'].replace('', '0')

    # Encoding the columns as per the specifications
    current_year = current_date.year
    df['start_recency_months'] = df['start_time'].astype(int).apply(lambda x: (current_year - x) * 12)
    df['end_recency_months'] = df['end_time'].astype(int).apply(lambda x: (current_year - x) * 12)

    return df

def create_institute_size_column(df):
    # Define the size mapping
    institute_size_mapping = {
        '51-200 employees': 1,
        '201-500 employees': 2,
        '501-1000 employees': 3,
        '1,001-5,000 employees': 4,
        '5,001-10,000 employees': 5,
        '10,001+ employees': 6
    }

    # Create the 'company_size' column based on the 'size'
    df['size'].fillna('0', inplace=True)
    df['school_size'] = df['size'].map(institute_size_mapping)

    return df

def calculate_institute_age_column(df):
    # Convert 'founded' to numeric
    df['founded'] = pd.to_numeric(df['founded'], errors='coerce')

    # Calculate the 'company_age_years' column
    df['school_age_years'] = df['founded'].apply(lambda x: 0 if x == 0 else 2023 - x)

    return df

def calculate_duration_column(df):
    # Calculate the 'duration' column
    df['duration'] = (df['end_time'].astype(int) - df['start_time'].astype(int)) * 12

    return df

def calculate_institute_change(df):
    # Sort the dataframe vy profile and start time
    df.sort_values(by=['profile_name', 'start_time'], ascending=[False, False], inplace=True)

    # Reset indexes
    df = df.reset_index(drop=True)

    # Initialize an empty list to store the values for the new "institute_change" column
    institute_change = []

    # Initialize a variable to keep track of the current profile
    current_profile = None
    previous_organization = None

    # Iterate over the rows of the DataFrame in reverse order
    for index in reversed(df.index):
        row = df.loc[index]
        if row['profile_name'] != current_profile:
            # If the profile has changed, set the institute_change value to -1
            institute_change.insert(0, -1)
            current_profile = row['profile_name']
            previous_organization = row['organisation']
        else:
            # Check if the organization has changed compared to the next row
            if row['organisation'] != previous_organization:
                institute_change.insert(0, 1)  # Organization changed
            else:
                if previous_organization is None:
                    institute_change.insert(0, -1)  # First organization in profile
                else:
                    institute_change.insert(0, 0)  # Organization did not change
            previous_organization = row['organisation']

    # Add the "institute_change" column to the DataFrame
    df['institute_change'] = institute_change

    return df

def calculate_cumulative_institute_changes(df):
    # Initialize a dictionary to store the cumulative counts for each profile
    cumulative_counts = {}
    
    # Initialize a dictionary to store the set of previous organizations for each profile
    previous_organizations = {}
    
    # Initialize a variable to keep track of the current profile
    current_profile = None
    
    # Initialize a variable to store the cumulative count
    cumulative_count = 0
    
    # Iterate over the rows of the DataFrame in reverse order
    for index in reversed(df.index):
        row = df.loc[index]
        if row['profile_name'] != current_profile:
            # If the profile has changed, reset the cumulative count to 1
            cumulative_count = 1
            current_profile = row['profile_name']
            previous_organizations[current_profile] = set()

        # Check if the current organization has not occurred in previous rows of the current profile
        if row['institute_change'] == 1 and row['organisation'] not in previous_organizations[current_profile]:
            cumulative_count += 1
                
        # Store the cumulative count for the current row in the dictionary
        cumulative_counts[index] = cumulative_count
        
        # Add the current organization to the set of previous organizations for the current profile
        previous_organizations[current_profile].add(row['organisation'])
    
    # Create a list of cumulative counts based on the DataFrame rows
    cumulative_count_list = [cumulative_counts[index] for index in df.index]
    
    # Add the cumulative count as a new column in the DataFrame
    df['cum_no_of_schools'] = cumulative_count_list

    return df

def clean_and_map_institute_headquarters(df):
    # Cleaning headquaters (Removing numbers and strings after comma)
    def clean_institute_headquarters(value, part):
        if pd.notna(value):
            parts = value.split(',')
            if len(parts) > part:
                return ''.join(filter(str.isalpha, parts[part]))
        return value

    df['headquaters_part1'] = df['headquaters'].apply(lambda x: clean_institute_headquarters(x, 0))
    df['headquaters_part2'] = df['headquaters'].apply(lambda x: clean_institute_headquarters(x, 1))

    # Defining country mapping function
    def get_institute_country_from_city(city_name):
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

    df['headquaters_part1'] = df['headquaters_part1'].apply(lambda city: get_institute_country_from_city(city))
    df['headquaters_part2'] = df['headquaters_part2'].apply(lambda city: get_institute_country_from_city(city))

    # Define function to check if headquarters is in Sri Lanka
    def institute_is_in_sri_lanka(row):
        if 'Sri Lanka' in row['headquaters_part1'] or 'Sri Lanka' in row['headquaters_part2']:
            return 1
        else:
            return 0

    df['headquaters'] = df.apply(institute_is_in_sri_lanka, axis=1)

    columns_to_drop = ['headquaters_part1', 'headquaters_part2']
    df.drop(columns=columns_to_drop, inplace=True)

    return df

def transform_and_reorder_institute_columns(df):
    # Drop unnecessary columns
    columns_to_drop = ['organisation', 'start_time', 'end_time', 'founded', 'size', 'institute_change']
    df.drop(columns=columns_to_drop, inplace=True)

    # Reorder the columns in the DataFrame
    desired_order = ['profile_name', 'course_level', 'duration', 'start_recency_months', 'end_recency_months', 'school_size', 
                     'school_age_years', 'headquaters', 'cum_no_of_schools']
    df = df[desired_order]

    return df

def transform_data(input_file_path):
    df = pd.read_json(input_file_path)

    df = normalize_education_data(df)
    df = map_course_level(df)
    df = merge_and_select_columns(df)
    df = transform_date_columns(df)
    df = create_institute_size_column(df)
    df = calculate_institute_age_column(df)
    df = calculate_duration_column(df)
    df = calculate_institute_change(df)
    df = calculate_cumulative_institute_changes(df)
    df = clean_and_map_institute_headquarters(df)
    df = transform_and_reorder_institute_columns(df)

    return df

# Example Usage:
input_file_path = 'data\\combined_data\\profiles_combined.json'
education_df = transform_data(input_file_path)

# Save the transformed DataFrame as a new CSV file if needed
education_df.to_csv('encoded_data\\education_encoded.csv', index=False)
