import scrapy
import json

class LinkedinstituteSpider(scrapy.Spider):
    name = "institutes"

    custom_settings = {
        'FEEDS': { 'data/scraped_institutes_data/%(name)s_%(time)s.json': { 'format': 'json' }}
    }  

    scraped_institutes = set()

    # Load the set of scraped companies from the file
    with open('data/combined_data/institutes_combined.json', 'r') as scraped_file:
        scraped_data = json.load(scraped_file)
        for entry in scraped_data:
            company_url = entry.get("url")  # Get the "name" field
            if company_url:
                scraped_institutes.add(company_url)  # Add the company name to the set                      
                              
    # List to store unique organization profile links
    new_institutes = set()

    # Read the JSON data from the file line by line
    with open('data/combined_data/profiles_combined.json', 'r') as json_file:
        json_data = json.load(json_file)
        for line_number, profile_data in enumerate(json_data, start=1):
            try:
                # Extract the "experience" section from the JSON data
                educations = profile_data.get("education", [])

                # Iterate through each experience entry and extract the "organisation_profile" if it exists
                for education in educations:
                    organization_profile = education.get("organisation_profile")
                    if organization_profile:
                        # Check if the company profile has not been scraped already
                        if organization_profile not in scraped_institutes:
                            new_institutes.add(organization_profile)

            except json.JSONDecodeError as e:
                print(f"Error in line {line_number}: {e}")
                print(f"Problematic line: {profile_data}")


    # Convert the set to a list if needed
    institutes_pages = list(new_institutes)

    if len(institutes_pages) != 0:

        def start_requests(self):
            institute_index_tracker = 0
            first_url = self.institutes_pages[institute_index_tracker]
            yield scrapy.Request(
                  url=first_url, 
                  callback=self.parse_response,
                  meta={'institute_index_tracker': institute_index_tracker, 'first_url': first_url}
            )

        def parse_response(self, response):

            institute_index_tracker = response.meta['institute_index_tracker']

            print('***************')
            print('****** Scraping page ' + str(institute_index_tracker+1) + ' of ' + str(len(self.institutes_pages)))
            print('***************')

            institute_item = {}

            institute_item['name'] = response.css('.top-card-layout__entity-info h1::text').get(default='').strip()
            
            if institute_index_tracker == 0:
                institute_item['url']  =  response.meta['first_url']
            else:
                institute_item['url']  =  response.meta['url']

            try:
                ## all institute details
                institute_details = response.css('.core-section-container__content .mb-2')

                #industry line
                try:
                    institute_industry_line = institute_details[1].css('.text-md::text').getall()
                    institute_item['industry'] = institute_industry_line[1].strip()
                except Exception as e:
                        print('institute_item --> founded', e)
                        institute_item['industry'] = ''

                #institute size line
                try:
                    institute_size_line = institute_details[2].css('.text-md::text').getall()
                    institute_item['size'] = institute_size_line[1].strip()
                except Exception as e:
                        print('institute_item --> size', e)
                        institute_item['size'] = ''

                #institute headquaters
                try:
                    institute_headquaters_line = institute_details[3].css('.text-md::text').getall()
                    institute_item['headquaters'] = institute_headquaters_line[1].strip()
                except Exception as e:
                        print('institute_item --> headquaters', e)
                        institute_item['headquaters'] = ''

                #institute Type
                try:
                    institute_type_line = institute_details[4].css('.text-md::text').getall()
                    institute_item['type'] = institute_type_line[1].strip()
                except Exception as e:
                        print('institute_item --> type', e)
                        institute_item['type'] = '' 

                try:
                    institute_founded_line = institute_details[5].css('.text-md::text').getall()[1].strip()            
                    year = int(institute_founded_line)
                    institute_item['founded'] = institute_founded_line
                except Exception as e:
                    print('institute_item --> founded', e)
                    institute_item['founded'] = ''

            except IndexError:

                print("Error: Skipped institute - Some details missing")

            yield institute_item

            institute_index_tracker = institute_index_tracker + 1

            if institute_index_tracker <= (len(self.institutes_pages)-1):
                next_url = self.institutes_pages[institute_index_tracker]

                yield scrapy.Request(
                    url=next_url, 
                    callback=self.parse_response, 
                    meta={'institute_index_tracker': institute_index_tracker, 'url': next_url})
                
    else:
        print("No new institutes to scrape.")

    

    