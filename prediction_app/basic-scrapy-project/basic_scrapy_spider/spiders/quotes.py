import scrapy
import json
import time
import requests
from urllib.parse import urlencode

class LinkedInPeopleProfileSpider(scrapy.Spider):
    name = "people"
    # Storing the scraped data
    custom_settings = {
         'FEEDS': { 'data/scraped_people_data/profile.json': { 'format': 'json', 'overwrite': True}}
    }

    # # Create the profile_list for chosen profiles by adding profiles to list manually  
    # API_KEY = '8ee501ea-5ccc-4913-a572-9fdc1bcdd9b4'
    # residential_status = 'true'

    def set_profile_link(self, profile_link):
        self.profile_link = profile_link

    # Starting a request
    def start_requests(self):
        # Scrape manually created profile_list for chosen profiles
        print('\n==============================')
        print(f'Scraping {self.profile_link}')
        print('==============================\n')
        linkedin_people_url = f'https://www.linkedin.com/in/{self.profile_link}/'
        api_key = self.settings.get('SCRAPEOPS_API_KEY')
        proxy_url = f'https://proxy.scrapeops.io/v1/?api_key={api_key}&url={linkedin_people_url}&residential=true'
        yield scrapy.Request(
            url=proxy_url, 
            callback=self.parse_profile, 
            meta={'profile': self.profile_link, 'linkedin_url': linkedin_people_url}
        )
        time.sleep(5)

    #   Scrape for subsets by looping through all the subsets 
    #   for subset_num, subset in enumerate(self.profiles_sublists, 1):
    #       total_subsets = len(self.profiles_sublists)
    #       print('/n==============================')
    #       print(f'Scraping Subset {subset_num} out of {total_subsets}')
    #       print('==============================/n')
    #       for item_num, profile in enumerate(subset, 1):
    #           linkedin_people_url = f'https://www.linkedin.com/in/{profile}/'
    #           total_items_in_subset = len(subset)
    #           print('/n==============================')
    #           print(f'Scraping Item {item_num} out of {total_items_in_subset} in Subset {subset_num} out of {total_subsets}')
    #           print('==============================/n')
    #           yield scrapy.Request(
    #               url=linkedin_people_url,
    #               callback=self.parse_profile,
    #               meta={'profile': profile, 'linkedin_url': linkedin_people_url}
    #           )
    #           time.sleep(10)
    #       time.sleep(10)

    def parse_profile(self, response):
        item = {}
        item['profile'] = response.meta['profile']
        item['url'] = response.meta['linkedin_url']

        """
            SUMMARY SECTION
        """
        summary_box = response.css("section.top-card-layout")
        item['name'] = summary_box.css("h1::text").get().strip()
        # item['description'] = summary_box.css("h2::text").get().strip()

        # Profile Location
        # try:
        #     item['location'] = summary_box.css('div.top-card__subline-item::text').get()
        # except:
        #     item['location'] = summary_box.css('span.top-card__subline-item::text').get().strip()
        #     if 'followers' in item['location'] or 'connections' in item['location']:
        #         item['location'] = ''

        # item['followers'] = ''
        # item['connections'] = ''

        # for span_text in summary_box.css('span.top-card__subline-item::text').getall():
        #     if 'followers' in span_text:
        #         item['followers'] = span_text.replace(' followers', '').strip()
        #     if 'connections' in span_text:
        #         item['connections'] = span_text.replace(' connections', '').strip()

        # """
        #     ABOUT SECTION
        # """
        # item['about'] = response.css('section.summary div.core-section-container__content p::text').get()

        """
            EXPERIENCE SECTION
        """
        item['experience'] = []
        experience_blocks = response.css('li.profile-section-card.experience-item')

        for block in experience_blocks:
            experience = {}

            # if inner_experience_blocks:
            #     # If inner list exists, process it
            #     for inner_block in inner_experience_blocks:
            #         inner_experience = {}

            #         # Position (Designation)
            #         try:
            #             inner_experience['position'] = inner_block.css("h3::text").get().strip()
            #         except Exception as e:
            #             print('inner_experience --> position', e)
            #             inner_experience['position'] = ''

            #         # Organisation
            #         try:
            #             if inner_block.css("h4 a::text").get():
            #                 inner_experience['organisation'] = inner_block.css("h4 a::text").get().strip()
            #             elif inner_block.css("h4::text").get():
            #                 inner_experience['organisation'] = inner_block.css("h4::text").get().strip()
            #         except Exception as e:
            #             print('inner_experience --> organisation', e)
            #             inner_experience['organisation'] = ''

            #         # Organisation profile url
            #         try:
            #             inner_experience['organisation_profile'] = inner_block.css('h4 a::attr(href)').get().split('?')[0]
            #         except Exception as e:
            #             print('inner_experience --> organisation_profile', e)
            #             inner_experience['organisation_profile'] = ''

            #         # Location
            #         try:
            #             inner_experience['location'] = inner_block.css('p.experience-group-position__location::text').get().strip()
            #         except Exception as e:
            #             print('inner_experience --> location', e)
            #             inner_experience['location'] = ''

            #         # # Total duration
            #         # try:
            #         #     inner_experience['total_duration'] = inner_block.css('p.experience-group-header__duration::text').get().strip()
            #         # except Exception as e:
            #         #     print('inner_experience --> total_duration', e)
            #         #     inner_experience['total_duration'] = ''                    

            #         # Time range
            #         try:
            #             date_ranges = inner_block.css('span.date-range time::text').getall()
            #             if len(date_ranges) == 2:
            #                 inner_experience['start_time'] = date_ranges[0]
            #                 inner_experience['end_time'] = date_ranges[1]
            #                 inner_experience['duration'] = inner_block.css('span.before\\:middot::text').get()
            #             elif len(date_ranges) == 1:
            #                 inner_experience['start_time'] = date_ranges[0]
            #                 inner_experience['end_time'] = 'present'
            #                 inner_experience['duration'] = inner_block.css('span.before\\:middot::text').get()
            #         except Exception as e:
            #             print('experience --> time ranges', e)
            #             experience['start_time'] = ''
            #             experience['end_time'] = ''
            #             experience['duration'] = ''

            #         # Description
            #         # try:
            #         #     description = ''.join(inner_block.css('p.show-more-less-text__text--more *::text').getall()).strip()
            #         #     inner_experience['description'] = description.replace('\n', ' ')
            #         # except Exception as e:
            #         #     print('inner experience --> description', e)
            #         #     try:
            #         #         description = ''.join(inner_block.css('p.show-more-less-text__text--less::text *::text').getall()).strip()
            #         #         inner_experience['description'] = description.replace('\n', ' ')
            #         #     except Exception as e:
            #         #         print('inner experience --> description', e)
            #         #         inner_experience['description'] = ''

            #         item['experience'].append(inner_experience)

            
            # Position
            try:
                experience['position'] = block.css("h3 span::text").get().strip()
            except Exception as e:
                print('experience --> position', e)
                experience['position'] = ''

            # organisation
            try:
                experience['organisation'] = block.css("h4 a span::text").get().strip()
            except Exception as e:
                print('experience --> organisation', e)
                try:
                    experience['organisation'] = block.css("h4 span::text").get().strip()
                except Exception as e:
                    print('experience --> organisation', e)
                    experience['organisation'] = ''
    
            # organisation profile url
            try:
                experience['organisation_profile'] = block.css('h4 a::attr(href)').get().split('?')[0]
            except Exception as e:
                print('experience --> organisation_profile', e)
                experience['organisation_profile'] = ''
                
            # location
            try:
                experience['location'] = block.css('p.experience-item__meta-item:nth-child(2)::text').get().strip()
            except Exception as e:
                print('experience --> location', e)
                experience['location'] = ''
                    
            # time range
            try:
                date_ranges = block.css('span.date-range time::text').getall()
                if len(date_ranges) == 2:
                    experience['start_time'] = date_ranges[0]
                    experience['end_time'] = date_ranges[1]
                    experience['duration'] = block.css('span.before\\:middot::text').get()
                elif len(date_ranges) == 1:
                    experience['start_time'] = date_ranges[0]
                    experience['end_time'] = 'present'
                    experience['duration'] = block.css('span.before\\:middot::text').get()
            except Exception as e:
                print('experience --> time ranges', e)
                experience['start_time'] = ''
                experience['end_time'] = ''
                experience['duration'] = ''

            # # description
            # try:
            #     description = ''.join(block.css('p.show-more-less-text__text--more *::text').getall()).strip()
            #     experience['description'] = description.replace('\n', ' ')
            # except Exception as e:
            #     print('experience --> description', e)
            #     try:
            #         description = ''.join(block.css('p.show-more-less-text__text--less::text *::text').getall()).strip()
            #         experience['description'] = description.replace('\n', ' ')
            #         # experience['description'] = block.css('p.show-more-less-text__text--less::text').get().strip()
            #     except Exception as e:
            #         print('experience --> description', e)
            #         experience['description'] = ''
            
            item['experience'].append(experience)

        
        inner_experience_blocks = response.css('li.profile-section-card.experience-group-position')

        if inner_experience_blocks:
            # If inner list exists, process it
            for inner_block in inner_experience_blocks:
                inner_experience = {}

                # Position (Designation)
                try:
                    inner_experience['position'] = inner_block.css("h3 span::text").get().strip()
                except Exception as e:
                    print('inner_experience --> position', e)
                    inner_experience['position'] = ''

                # organisation
                try:
                    inner_experience['organisation'] = inner_block.css("h4 a span::text").get().strip()
                except Exception as e:
                    print('experience --> organisation', e)
                    try:
                        inner_experience['organisation'] = inner_block.css("h4 span::text").get().strip()
                    except Exception as e:
                        print('experience --> organisation', e)
                        inner_experience['organisation'] = ''

                # Organisation profile url
                try:
                    inner_experience['organisation_profile'] = inner_block.css('h4 a::attr(href)').get().split('?')[0]
                except Exception as e:
                    print('inner_experience --> organisation_profile', e)
                    inner_experience['organisation_profile'] = ''

                # Location
                try:
                    inner_experience['location'] = inner_block.css('p.experience-item__meta-item:nth-child(2)::text').get().strip()
                except Exception as e:
                    print('inner_experience --> location', e)
                    inner_experience['location'] = ''

                # # Total duration
                # try:
                #     inner_experience['total_duration'] = inner_block.css('p.experience-group-header__duration::text').get().strip()
                # except Exception as e:
                #     print('inner_experience --> total_duration', e)
                #     inner_experience['total_duration'] = ''                    

                # Time range
                try:
                    date_ranges = inner_block.css('span.date-range time::text').getall()
                    if len(date_ranges) == 2:
                        inner_experience['start_time'] = date_ranges[0]
                        inner_experience['end_time'] = date_ranges[1]
                        inner_experience['duration'] = inner_block.css('span.before\\:middot::text').get()
                    elif len(date_ranges) == 1:
                        inner_experience['start_time'] = date_ranges[0]
                        inner_experience['end_time'] = 'present'
                        inner_experience['duration'] = inner_block.css('span.before\\:middot::text').get()
                except Exception as e:
                    print('experience --> time ranges', e)
                    inner_experience['start_time'] = ''
                    inner_experience['end_time'] = ''
                    inner_experience['duration'] = ''

                # Description
                # try:
                #     description = ''.join(inner_block.css('p.show-more-less-text__text--more *::text').getall()).strip()
                #     inner_experience['description'] = description.replace('\n', ' ')
                # except Exception as e:
                #     print('inner experience --> description', e)
                #     try:
                #         description = ''.join(inner_block.css('p.show-more-less-text__text--less::text *::text').getall()).strip()
                #         inner_experience['description'] = description.replace('\n', ' ')
                #     except Exception as e:
                #         print('inner experience --> description', e)
                #         inner_experience['description'] = ''

                item['experience'].append(inner_experience)

        
        """
            EDUCATION SECTION
        """
        item['education'] = []

        education_blocks = response.css('li.education__list-item')

        for block in education_blocks:
            education = {}

            # organisation
            try:
                if block.css("h3 a::text").get():
                    education['organisation'] = block.css("h3 a::text").get().strip()
                elif block.css("h3::text").get():
                    education['organisation'] = block.css("h3::text").get().strip()
            except Exception as e:
                print("education --> organisation", e)
                education['organisation'] = ''

            # organisation profile url
            try:
                education['organisation_profile'] = block.css('a::attr(href)').get().split('?')[0]
            except Exception as e:
                print("education --> organisation_profile", e)
                education['organisation_profile'] = ''

            # course details
            try:
                education['course_details'] = ''
                for text in block.css('h4 span::text').getall():
                    education['course_details'] = education['course_details'] + text.strip() + ' '
                education['course_details'] = education['course_details'].strip()
            except Exception as e:
                print("education --> course_details", e)
                education['course_details'] = ''

            # # description
            # try:
            #     education['description'] = block.css('div.education__item--details p::text').get().strip()
            # except Exception as e:
            #     print("education --> description", e)
            #     education['description'] = ''

            # try:
            #     education_description = ''.join(block.css('p.show-more-less-text__text--more *::text').getall()).strip()
            #     experience['description'] = education_description.replace('\n', ' ')
            # except Exception as e:
            #     print('experience --> description', e)
            #     try:
            #         education_description = ''.join(block.css('p.show-more-less-text__text--less *::text').getall()).strip()
            #         experience['description'] = education_description.replace('\n', ' ')
            #     except Exception as e:
            #         print('experience --> description', e)
            #         experience['description'] = ''
         
            # time range
            try:
                date_ranges = block.css('span.date-range time::text').getall()
                if len(date_ranges) == 2:
                    education['start_time'] = date_ranges[0]
                    education['end_time'] = date_ranges[1]
                elif len(date_ranges) == 1:
                    education['start_time'] = date_ranges[0]
                    education['end_time'] = 'present'
            except Exception as e:
                print("education --> time_ranges", e)
                education['start_time'] = ''
                education['end_time'] = ''

            item['education'].append(education)

        yield item

        # """
        #     Certification SECTION
        # """
        # item['certifications'] = []
        # certification_blocks = response.css('ul.certifications__list li')
        # for block in certification_blocks:
        #     certification = {}

        #     ## certification_title
        #     try:
        #         certification['certification_title'] = block.css("h3 a::text").get().strip()
        #     except Exception as e:
        #         print("certification --> certification_title", e)
        #         certification['certification_title'] = ''


        #     ## certification_issuer
        #     try:
        #         certification['certification_issuer'] = block.css('h4 a::text').get().strip()
        #     except Exception as e:
        #         print("certification --> certification_issuer", e)
        #         certification['certification_issuer'] = ''


        #     ## description_issued_date
        #     try:
        #         certification['issued_date'] = block.css('span.certifications__start-date time::text').get().strip()
        #     except Exception as e:
        #         print("certification --> issued_date", e)
        #         certification['issued_date'] = ''

        #     item['certifications'].append(certification)

        # """
        #     Publications SECTION
        # """
        # item['publications'] = []
        # publications_blocks = response.css('ul.publications__list li')
        # for block in publications_blocks:
        #     publications = {}

        #     ## publication_title
        #     try:
        #         publications['publication_title'] = block.css("h3::text").get().strip()
        #     except Exception as e:
        #         print("publications --> publication_title", e)
        #         publications['publication_title'] = ''


        #     ## publication_link
        #     try:
        #         publications['publication_link'] = block.css('h4 span.text-color-text-low-emphasis::text').get().strip()
        #     except Exception as e:
        #         print("awards --> publication_link", e)
        #         publications['publication_link'] = ''


        #     ## awarded_date
        #     try:
        #         publications['published_date'] = block.css('span.date-range time::text').get().strip()
        #     except Exception as e:
        #         print("publications --> published_date", e)
        #         publications['published_date'] = ''

        #     item['publications'].append(publications)

        # """
        #     Honors and awards SECTION
        # """
        # item['awards'] = []
        # award_blocks = response.css('ul.awards__list li')
        # for block in award_blocks:
        #     awards = {}

        #     ## award_title
        #     try:
        #         awards['award_title'] = block.css("h3::text").get().strip()
        #     except Exception as e:
        #         print("awards --> award_title", e)
        #         awards['award_title'] = ''

        #     ## award_giver
        #     try:
        #         awards['award_giver'] = block.css('h4::text').get().strip()
        #     except Exception as e:
        #         print("awards --> award_giver", e)
        #         awards['award_giver'] = ''

        #     ## awarded_date
        #     try:
        #         awards['awarded_date'] = block.css('span.date-range time::text').get().strip()
        #     except Exception as e:
        #         print("certification --> awarded_date", e)
        #         awards['awarded_date'] = ''

        #     item['awards'].append(awards)

        # yield item
