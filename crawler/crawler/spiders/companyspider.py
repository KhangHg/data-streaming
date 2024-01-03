import scrapy

class CompanyspiderSpider(scrapy.Spider):
    name = "companyspider"
    #allowed_domains = ["www.itviec.com"]
    start_urls = ['https://itviec.com/jobs-company-index?gclid=CjwKCAjw-eKpBhAbEiwAqFL0mr7rB3hs8ZMO0wucEzAG0bXUOMmtdxzOocTb4rj5216_awW1jx31hRoCYa0QAvD_BwE&utm_campaign=gsn_brand_hn&utm_medium=cpc&utm_source=google&utm_term=it+vi%E1%BB%87c']

    
    def parse(self, response):
        company_links = response.css('li.skill-tag__item a::attr(href)').getall()
        company_urls = ["https://itviec.com" + link for link in company_links]
        for company_url in company_urls:
            company_url = "https://itviec.com" + company_url
        yield from response.follow_all(company_urls, self.parse_company)
        
       

    def parse_company(self, response):
        def extract_with_css(query):
            return response.css(query).get(default = "").strip()

        size = response.css('div.col-md-4.d-flex.flex-md-column.justify-content-between.border-bottom-dotted-sm.ipy-2.ipy-md-0 div.normal-text::text')[1].get().strip(), 
        country = response.css('div.col-md-4.d-flex.flex-md-column.justify-content-between.border-bottom-dotted-sm.ipy-2.ipy-md-0 div.d-flex.align-items-center div.d-inline-block span::text').get().strip(),
        working_day = response.css('div.col-md-4.d-flex.flex-md-column.justify-content-between.border-bottom-dotted-sm.ipy-2.ipy-md-0 div.normal-text::text')[2].get().strip(),

        yield {
            # "company_url" :  "https://itviec.com" + company.css('a.mkt-track.skill-tag__link.normal-text.text-it-black').attrib['href'],
            "company_name": extract_with_css('h1.text-center.text-md-start.ipt-4.ipb-2.ipt-md-0::text'),
            "company_url" : response.url,
            "type" : extract_with_css('div.col-md-4.d-flex.flex-md-column.justify-content-between.border-bottom-dotted-sm.ipy-2.ipy-md-0 div.normal-text::text'), 
            "size" : size,
            "working_day": working_day,
            "country": country,
            # "overview" : 
            # "key_skill" :
            "location" : extract_with_css('div.d-flex.igap-2 span.text-break::text'),
            "job_quantity": extract_with_css('h2.ipb-6::text')    
        }