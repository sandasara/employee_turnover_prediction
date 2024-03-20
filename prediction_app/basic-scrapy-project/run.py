import subprocess
import json
import pandas as pd

# def run_spider(spider_name, profile_name=None):
#     """
#     Run the people, company, and institute spiders to scraped profile, company, and institute data.

#     Parameters:
#     spider_name: Name of the spider. Defined relevent in spider file
#     profile_name: The profile name of the employee. Taken as a user input.

#     Returns:
#     json file with scraped data.
#     The file is stored in data\scraped_people_data
#     """
#     args = ['scrapy', 'crawl', spider_name]
    
#     if profile_name:
#         args.extend(['-a', f'profile_name={profile_name}'])
    
#     subprocess.run(args)
 
def run_additional_scripts():
    """
    Run encoding scripts and final prediction scripts.

    Returns:
    Final encoded dataset
    Final Predictions
    """
    subprocess.run(['python', 'organisation_encode.py'])
    subprocess.run(['python', 'education_encode.py'])
    subprocess.run(['python', 'final_encoding.py'])
    subprocess.run(['python', 'predicting.py'])

def get_Profile(profile_name):
    """
    Checks and retrieve profile data from a existing file instead of scraping.

    profile_name: The profile name of the employee. Taken as a user input.

    Returns:
    json file that contains data for the matching profile name.
    The file is stored in scraped_people_data folder.
    """
    # Check for a matching profilee in test_profiles file
    df = pd.read_json(r'data\test\test_profiles.json')
    matching_row = df.loc[df['profile'] == profile_name]

    # If a matching row is found, create a new DataFrame with that single row
    if not matching_row.empty:
        print("\nData retrieving and encoding started.")
        extracted_json = matching_row.to_dict(orient='records')[0]

        result_json = {
            "profile": extracted_json["profile"],
            "url": extracted_json["url"],
            "name": extracted_json["name"],
            "experience": extracted_json["experience"],
            "education": extracted_json["education"]
        }

        result_json_str = json.dumps(result_json, indent=4)

        # Save the result to a new file
        with open('data\\scraped_people_data\\profile.json', 'w') as outfile:
            outfile.write(result_json_str)

        # Load the JSON file
        with open('data\\scraped_people_data\\profile.json', 'r') as f:
            data = json.load(f)

        # Wrap the object in a list
        data_list = [data]

        # Save the modified data back to a file
        with open('data\\scraped_people_data\\profile.json', 'w') as f:
            json.dump(data_list, f, indent=4)
    else:
        print("\nCheck profile name again.\n")

if __name__ == '__main__':
    # Get profile link from user input
    profile_name = input("Enter the profile name: ")

    # Find the the matching profile data from scraped data.
    get_Profile(profile_name)

    # # Run spiders to scrape profile data
    # run_spider('people', profile_name)
    # run_spider('companies')
    # run_spider('institutes')

    # Run encoding and predicting scripts
    run_additional_scripts()