import scrapy
from urllib.parse import urlparse, parse_qs

class QuestionsSpider(scrapy.Spider):
    name = "questions"
    allowed_domains = ["stackoverflow.com"]
    start_urls = ["https://stackoverflow.com/questions?tab=Active"]
    # start_urls = ["https://stackoverflow.com/questions"]

    def parse(self, response):
        self.logger.debug("inside parse")
        for question in response.css("#questions .s-post-summary"):
            # self.logger.debug(question.css("h3.s-post-summary--content-title a::text").get())
            question_url = "https://stackoverflow.com"+question.css("h3 a::attr(href)").get()
            answer_count =  question.css(".s-post-summary--stats-item:nth-child(2) .s-post-summary--stats-item-number::text").get()

            if answer_count and int(answer_count) > 0:
                self.logger.debug("inside if going to this link: " + question_url)
                yield response.follow(question_url, self.parse_question, meta={
                    "question": question.css("h3.s-post-summary--content-title a::text").get(),
                    "link": question_url,
                    "votes": question.css(".s-post-summary--stats-item:nth-child(1) .s-post-summary--stats-item-number::text").get(),
                    "answers": answer_count,
                    "views": question.css(".s-post-summary--stats-item:nth-child(3) .s-post-summary--stats-item-number::text").get(),
                    "tags": question.css(".s-post-summary--meta .s-post-summary--meta-tags .s-tag::text").getall(),
                    "username": question.css(".s-post-summary--meta .s-user-card .s-user-card--info a::text").get(),
                    "user_link": "https://stackoverflow.com"+question.css(".s-post-summary--meta .s-user-card .s-user-card--info a::attr(href)").get(),
                    "question_post_time": question.css(".s-post-summary--meta .s-user-card .s-user-card--time .relativetime::attr(title)").get(),
                })
            else:
                # If there are 0 answers, just yield the question data without the answers
                yield {
                    "question": question.css("h3.s-post-summary--content-title a::text").get(),
                    "link": question_url,
                    "votes": question.css(".s-post-summary--stats-item:nth-child(1) .s-post-summary--stats-item-number::text").get(),
                    "answers": answer_count,
                    "views": question.css(".s-post-summary--stats-item:nth-child(3) .s-post-summary--stats-item-number::text").get(),
                    "tags": question.css(".s-post-summary--meta .s-post-summary--meta-tags .s-tag::text").getall(),
                    "username": question.css(".s-post-summary--meta .s-user-card .s-user-card--info a::text").get(),
                    "user_link": "https://stackoverflow.com"+question.css(".s-post-summary--meta .s-user-card .s-user-card--info a::attr(href)").get(),
                    "question_post_time": question.css(".s-post-summary--meta .s-user-card .s-user-card--time .relativetime::attr(title)").get(),
                    "accepted_answer": [], 
                    "suggested_answer": []
                }

        next_page = response.css(".s-pagination [rel='next']::attr(href)").get()
        if next_page is not None:
            self.logger.debug(">>>>>>>>>>>>>>  Next page URL: " + next_page)
            
            # Parse the URL to get query parameters
            parsed_url = urlparse(next_page)
            query_params = parse_qs(parsed_url.query)
            
            # Check if 'page' parameter exists and if it's equal to 3
            if query_params.get('page', [None])[0] == '100':
                self.logger.debug("================================= Reached page 100, stopping spider.")
                return 
            
            # Otherwise, follow the next page
            yield response.follow(next_page, callback=self.parse)

    def parse_question(self, response):
        self.logger.debug("inside parse_question")
        accepted_answer_li = []
        # accepted_answer = ' '.join([t.strip() for t in response.css('#answers .accepted-answer .s-prose *::text').getall() if t.strip()]).strip()
        for answer in response.css('[itemprop="acceptedAnswer"]'):
            accepted_answer_dict = {}
            texts = answer.css('.s-prose *::text').getall()
            if texts:
                accepted_answer_dict["accepted_answer"] = ' '.join([t.strip() for t in texts if t.strip()])
                accepted_answer_dict["post_time"] = answer.css('div.user-action-time .relativetime::attr(title)').get()
                accepted_answer_dict["username"] = answer.css('.user-info .user-details a::text').get()
                accepted_answer_dict["user_link"] =  "https://stackoverflow.com"+answer.css('.user-info .user-details a::attr(href)').get()
                accepted_answer_dict["reputation_score"] = answer.css('.user-info .user-details .reputation-score::text').get() or '0'
                accepted_answer_dict["gold_badge_count"] = answer.css('.user-info .user-details [title*="gold"] .badgecount::text').get() or '0'
                accepted_answer_dict["silver_badge_count"] = answer.css('.user-info .user-details [title*="silver"] .badgecount::text').get() or '0'
                accepted_answer_dict["bronze_badge_count"] = answer.css('.user-info .user-details [title*="bronze"] .badgecount::text').get() or '0'
                accepted_answer_li.append(accepted_answer_dict)

        suggested_answers_li = []
        for answer in response.css('[itemprop="suggestedAnswer"]'):
            suggested_answer_dict = {}
            texts = answer.css('.s-prose *::text').getall()
            if texts:
                suggested_answer_dict["suggested_answer"] = ' '.join([t.strip() for t in texts if t.strip()])
                suggested_answer_dict["post_time"] = answer.css('div.user-action-time .relativetime::attr(title)').get()
                suggested_answer_dict["username"] = answer.css('.user-info .user-details a::text').get()
                suggested_answer_dict["user_link"] =  "https://stackoverflow.com"+answer.css('.user-info .user-details a::attr(href)').get()
                suggested_answer_dict["reputation_score"] = answer.css('.user-info .user-details .reputation-score::text').get() or '0'
                suggested_answer_dict["gold_badge_count"] = answer.css('.user-info .user-details [title*="gold"] .badgecount::text').get()  or '0'
                suggested_answer_dict["silver_badge_count"] = answer.css('.user-info .user-details [title*="silver"] .badgecount::text').get() or '0'
                suggested_answer_dict["bronze_badge_count"] = answer.css('.user-info .user-details [title*="bronze"] .badgecount::text').get() or '0'

                suggested_answers_li.append(suggested_answer_dict)

        yield {
            "question": response.meta.get("question"),
            "link": response.meta.get("link"),
            "votes": response.meta.get("votes"),
            "answers": response.meta.get("answers"),
            "views": response.meta.get("views"),
            "tags": response.meta.get("tags"),
            "username": response.meta.get("username"),
            "user_link": response.meta.get("user_link"),
            "question_post_time": response.meta.get("question_post_time"),
            "accepted_answer": accepted_answer_li, 
            "suggested_answer": suggested_answers_li
        }
            
# scrapy crawl questions -O test.json
