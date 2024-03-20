import scrapy
import json
import glob
import pandas as pd
import openpyxl

class LinkedCompanySpider(scrapy.Spider):
    name = "companies"

    custom_settings = {
        'FEEDS': { 'data/scraped_companies_data/%(name)s_%(time)s.json': { 'format': 'json' }}
    }  

    scraped_companies = set()
    new_companies = set()

    # Load the set of scraped companies from the file
    with open('data/combined_data/companies_combined.json', 'r') as json_file:
        json_data = json.load(json_file)
        for item in json_data:
            company = item.get("url")  # Get the "name" field
            if company:
                scraped_companies.add(company)  # Add the company name to the set
                                
    # Read the JSON data from the file line by line
    with open('data/combined_data/profiles_combined.json', 'r') as json_file:
        json_data = json.load(json_file)
        for line_number, profile_data in enumerate(json_data, start=1):
            try:
                # Extract the "experience" section from the JSON data
                experiences = profile_data.get("experience", [])

                # Iterate through each experience entry and extract the "organisation_profile" if it exists
                for experience in experiences:
                    organization_profile = experience.get("organisation_profile")
                    if organization_profile:
                        if organization_profile not in scraped_companies:
                            new_companies.add(organization_profile)                       

            except json.JSONDecodeError as e:
                print(f"Error in line {line_number}: {e}")
                print(f"Problematic line: {profile_data}")


    # Convert the set to a list if needed
    company_pages = list(new_companies)

    if len(company_pages) != 0:

        def start_requests(self):
            company_index_tracker = 0
            first_url = self.company_pages[company_index_tracker]
            yield scrapy.Request(
                  url=first_url, 
                  callback=self.parse_response,
                  meta={'company_index_tracker': company_index_tracker, 'first_url': first_url}
            )

        def parse_response(self, response):
            company_index_tracker = response.meta['company_index_tracker']

            print('***************')
            print('****** Scraping page ' + str(company_index_tracker+1) + ' of ' + str(len(self.company_pages)))
            print('***************')

            company_item = {}

            company_item['name'] = response.css('.top-card-layout__entity-info h1::text').get(default='').strip()
            
            if company_index_tracker == 0:
                company_item['url']  =  response.meta['first_url']
            else:
                company_item['url']  =  response.meta['url']

            try:
                ## all company details
                company_details = response.css('.core-section-container__content .mb-2')

                #industry line
                try:
                    company_industry_line = company_details[1].css('.text-md::text').getall()
                    company_item['industry'] = company_industry_line[1].strip()
                except Exception as e:
                        print('company_item --> founded', e)
                        company_item['industry'] = ''

                #company size line
                try:
                    company_size_line = company_details[2].css('.text-md::text').getall()
                    company_item['size'] = company_size_line[1].strip()
                except Exception as e:
                        print('company_item --> size', e)
                        company_item['size'] = ''

                #company headquaters
                try:
                    company_headquaters_line = company_details[3].css('.text-md::text').getall()
                    company_item['headquaters'] = company_headquaters_line[1].strip()
                except Exception as e:
                        print('company_item --> headquaters', e)
                        company_item['headquaters'] = ''

                #company Type
                try:
                    company_type_line = company_details[4].css('.text-md::text').getall()
                    company_item['type'] = company_type_line[1].strip()
                except Exception as e:
                        print('company_item --> type', e)
                        company_item['type'] = '' 

                try:
                    company_founded_line = company_details[5].css('.text-md::text').getall()[1].strip()
                    # try:                
                    year = int(company_founded_line)
                    company_item['founded'] = company_founded_line
                    # except Exception as ein:
                    #     company_item['founded'] = ''
                    #     company_item['specialties'] = company_founded_line
                except Exception as e:
                    print('company_item --> founded', e)
                    company_item['founded'] = ''

            except IndexError:

                print("Error: Skipped Company - Some details missing")

            yield company_item

            company_index_tracker = company_index_tracker + 1

            if company_index_tracker <= (len(self.company_pages)-1):
                next_url = self.company_pages[company_index_tracker]

                yield scrapy.Request(
                    url=next_url, 
                    callback=self.parse_response, 
                    meta={'company_index_tracker': company_index_tracker, 'url': next_url})

    else:
        print("No new companies to scrape.")
    