import scrapy
# from Crawler.items import CrawlerItem

class JobSpider(scrapy.Spider):
    name = "jobspider"
    # allowed_domains = ["www.topcv.vn"]
    headers = {'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    start_urls = [
        "https://itviec.com/it-jobs?gclid=CjwKCAjw-eKpBhAbEiwAqFL0mr7rB3hs8ZMO0wucEzAG0bXUOMmtdxzOocTb4rj5216_awW1jx31hRoCYa0QAvD_BwE&utm_campaign=gsn_brand_hn&utm_medium=cpc&utm_source=google&utm_term=it+vi%E1%BB%87c&job_selected=senior-it-quality-control-officer-mb-ageas-life-4701&page=3",
        # "https://www.topcv.vn/tim-viec-lam-it-phan-mem-c10026?sort=up_top"
    ]

    def parse(self, response):
        headers = {'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

        jobs = response.css('div.ipy-2')
        for job in jobs:
        
            job_item = {
                'job_name': job.css('h3.imt-3 a::text').get().strip(),
                'job_url': "https://itviec.com" + job.css('h3.imt-3 a::attr(href)').get().strip(),
                'type': job.css('div.d-flex.align-items-center.text-dark-grey.imt-1 span::text')[0].get().strip(),
                'location': job.css('div.d-flex.align-items-center.text-dark-grey.imt-1 span::text')[1].get().strip(),
                'company': job.css('div.imy-3.d-flex.align-items-center span.ims-2.small-text a.text-rich-grey::text').get().strip(),
                'tag': job.css('div.imt-3.imb-2 a.text-reset div.itag.itag-light.itag-sm::text').get().strip(),
                'post_time': job.css('div.d-flex.align-items-center.justify-content-between.position-relative span.small-text.text-dark-grey::text').get().strip(),
            }
            yield job_item
            self.log(job_item)
        
        next_page =  response.css("div.page.next a::attr(href)").get()
        if next_page is not None:
            next_page_url = "https://itviec.com" + next_page
            # next_page = response.urljoin(next_page)
            # yield scrapy.Request(next_page, callback=self.parse)
            yield scrapy.follow(next_page_url,headers = headers,callback=self.parse)
            
        pass
    
    # start_urls = [
    #     "http://quotes.toscrape.com/page/1/",
    # ]

    # def parse(self, response):
    #     # Extract quotes and authors
    #     for quote in response.css("div.quote"):
    #         yield {
    #             "text": quote.css("span.text::text").get(),
    #             "author": quote.css("span small::text").get(),
    #         }

    #     # Follow pagination link to the next page
    #     next_page = response.css("li.next a::attr(href)").get()
    #     if next_page is not None:
    #         yield response.follow(next_page, callback=self.parse)
