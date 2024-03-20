# import scrapy
# import json

# class LinkedOrganisationSpider(scrapy.Spider):
#     name = "organisations"

#     custom_settings = {
#         'FEEDS': { 'data/scraped_companies_data/%(name)s_%(time)s.jsonl': { 'format': 'jsonlines',}}
#     }  
#     # FEEDS = {
#     #     'company.csv': {'format': 'csv', 'overwrite': False}
#     # }

#     # Load the set of scraped companies from the file
#     scraped_companies = set()

#     with open('data/companies_combined.jsonl', 'r') as scraped_file:
#         for line in scraped_file:
#             data = json.loads(line)
#             company_name = data.get("name")  # Get the "name" field
#             if company_name:
#                 scraped_companies.add(company_name)  # Add the company name to the set

#     # List to store unique organization profile links
#     new_companies = set()

#     # Read the JSON data from the file line by line
#     with open('data/clean_profiles.jsonl', 'r') as json_file:
#         for line_number, line in enumerate(json_file, start=1):
#             try:
#                 # Load each line as a JSON object
#                 profile_data = json.loads(line)
                
#                 # Extract the "experience" section from the JSON data
#                 experiences = profile_data.get("experience", [])
                
#                 # Iterate through each experience entry and extract the "organisation profile" if it exists
#                 for experience in experiences:
#                     organization_profile = experience.get("organisation_profile")
#                     organization = experience.get("organisation")
#                     if organization_profile:
#                         # Check if the company profile has not been scraped already
#                         if organization not in scraped_companies:
#                             new_companies.add(organization_profile)

#             except json.JSONDecodeError as e:
#                 print(f"Error in line {line_number}: {e}")
#                 print(f"Problematic line: {line}")

#     # Convert the set to a list if needed

#     company_pages = list(new_companies)

#     # company_sublists = []
#     # chunk_size = 10

#     # for i in range(0, len(company_pages), chunk_size):
#     #     sublist = company_pages[i:i + chunk_size]
#     #     company_sublists.append(sublist)

#     # if len(company_sublists) != 0:

#     # company_pages = ["https://www.linkedin.com/company/mas-holdings/",
#     #                  "https://www.linkedin.com/company/ndbbank/",
#     #                  ]

#     if len(company_pages) != 0:

#         def start_requests(self):
#             company_index_tracker = 0
#             first_url = self.company_pages[company_index_tracker]
#             yield scrapy.Request(
#                   url=first_url, 
#                   callback=self.parse_response,
#                   meta={'company_index_tracker': company_index_tracker, 'first_url': first_url}
#             )

#             # for subset_index, subset in enumerate(self.company_sublists):
#             #     yield scrapy.Request(
#             #         url=subset[0],
#             #         callback=self.parse_response,
#             #         meta={'company_index_tracker': 0, 'subset_index': subset_index}
#             #     )

#         def parse_response(self, response):

#             # company_index_tracker = response.meta['company_index_tracker']
#             company_index_tracker = response.meta['company_index_tracker']
#             # subset_index = response.meta['subset_index']

#             print('***************')
#             print('****** Scraping page ' + str(company_index_tracker+1) + ' of ' + str(len(self.company_pages)))
#             print('***************')

#             company_item = {}

#             company_item['name'] = response.css('.top-card-layout__entity-info h1::text').get(default='').strip()
#             # company_item['summary'] = response.css('.top-card-layout__entity-info h4 span::text').get(default='').strip()

#             if company_index_tracker == 0:
#                 company_item['url']  =  response.meta['first_url']
#             else:
#                 company_item['url']  =  response.meta['url']

#             try:
#                 ## all company details
#                 company_details = response.css('.core-section-container__content .mb-2')

#                 # #web url
#                 # try:
#                 #     company_industry_line = company_details[0].css('a::text').get()
#                 #     company_item['url'] = company_industry_line.strip()
#                 # except Exception as e:
#                 #     print('company_item --> url', e)
#                 #     company_item['url'] = ''

#                 #industry line
#                 try:
#                     company_industry_line = company_details[1].css('.text-md::text').getall()
#                     company_item['industry'] = company_industry_line[1].strip()
#                 except Exception as e:
#                         print('company_item --> founded', e)
#                         company_item['founded'] = ''

#                 #company size line
#                 try:
#                     company_size_line = company_details[2].css('.text-md::text').getall()
#                     company_item['size'] = company_size_line[1].strip()
#                 except Exception as e:
#                         print('company_item --> size', e)
#                         company_item['size'] = ''

#                 #company headquaters
#                 try:
#                     company_headquaters_line = company_details[3].css('.text-md::text').getall()
#                     company_item['headquaters'] = company_headquaters_line[1].strip()
#                 except Exception as e:
#                         print('company_item --> headquaters', e)
#                         company_item['headquaters'] = ''

#                 #company type
#                 try:
#                     company_type_line = company_details[4].css('.text-md::text').getall()
#                     company_item['type'] = company_type_line[1].strip()
#                 except Exception as e:
#                         print('company_item --> type', e)
#                         company_item['type'] = '' 

#                 try:
#                     company_founded_line = company_details[5].css('.text-md::text').getall()[1].strip()
#                     # try:                
#                     year = int(company_founded_line)
#                     company_item['founded'] = company_founded_line
#                     # except Exception as ein:
#                     #     company_item['founded'] = ''
#                     #     company_item['specialties'] = company_founded_line
#                 except Exception as e:
#                     print('company_item --> founded', e)
#                     company_item['founded'] = ''            
#                 # # if len(company_details) >= 9:
#                 # # company founded
#                 # try:
#                 #     company_founded_line = company_details[5].css('.text-md::text').getall()                
#                 #     company_item['founded'] = company_founded_line[1].strip()
#                 # except Exception as e:
#                 #     print('company_item --> founded', e)
#                 #     company_item['founded'] = ''
                    

#                 # try:
#                 #     company_specialities_line = company_details[6].css('.text-md::text').getall()
#                 #     company_item['specialties'] = company_specialities_line[1].strip()
#                 # except Exception as e:
#                 #     print('company_item --> specialties', e)
#                 #     company_item['specialties'] = ''

#                 # if len(company_details) <= 8:
#                 #     #company founded
#                 #     company_founded_or_specialties_line = company_details[5].css('.text-md::text').getall()

#                 #     company_founded_or_specialties_topic = company_details[5].css('dt::text').get().strip().lower()

#                 #     if company_founded_or_specialties_topic == 'founded':
#                 #         try:           
#                 #             company_item['founded'] = company_founded_or_specialties_line[1].strip()
#                 #             company_item['specialties'] = ''
#                 #         except Exception as e:
#                 #             print('company_item --> founded', e)
#                 #             company_item['founded'] = ''
#                 #             company_item['specialties'] = ''

#                 #     elif company_founded_or_specialties_topic == 'specialties': 
#                 #         try:
#                 #             company_item['founded'] = ''           
#                 #             company_item['specialties'] = company_founded_or_specialties_line[1].strip()
#                 #         except Exception as e:
#                 #             print('company_item --> specialties', e)
#                 #             company_item['founded'] = ''
#                 #             company_item['specialties'] = ''
#             except IndexError:

#                 print("Error: Skipped Company - Some details missing")

#             yield company_item

#             company_index_tracker = company_index_tracker + 1

#             if company_index_tracker <= (len(self.company_pages)-1):
#                 next_url = self.company_pages[company_index_tracker]

#                 yield scrapy.Request(
#                     url=next_url, 
#                     callback=self.parse_response, 
#                     meta={'company_index_tracker': company_index_tracker, 'url': next_url})

#             # yield company_item

#             # company_index_tracker = company_index_tracker + 1

#             # if company_index_tracker < len(self.company_sublists[subset_index]):
#             #     next_url = self.company_sublists[subset_index][company_index_tracker]

#             #     yield scrapy.Request(
#             #         url=next_url,
#             #         callback=self.parse_response,
#             #         meta={'company_index_tracker': company_index_tracker, 'subset_index': subset_index}
#             #     )

#     else:
#         print("No new companies to scrape.")
    

    