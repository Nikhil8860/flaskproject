import requests
from fake_useragent import UserAgent

ua = UserAgent()


class Scraper:
    def __init__(self, base_url):
        self.url = base_url
        self.headers = dict()
        self.headers[
            "user-agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
        self.headers[
            "accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
        self.headers['accept-language'] = "en-US,en;q=0.9"
        self.headers['cache-control'] = "max-age=0"
        self.headers['ect'] = "4g"
        self.headers['rtt'] = "200"
        self.headers['sec-fetch-dest'] = 'document'
        self.headers['sec-fetch-mode'] = 'navigate'
        self.headers['sec-fetch-site'] = 'none'
        self.headers['sec-fetch-user'] = '?1'
        self.headers['upgrade-insecure-requests'] = '1'
        # self.headers["referer"]: "https://www.google.com"

    def get_url_data(self):
        with requests.session() as session:
            session.headers.update(self.headers)
            r = requests.get(self.url, headers=session.headers)
            # Simple check to check if page was blocked (Usually 503)
            if r.status_code > 500:
                if "To discuss automated access to Amazon data please contact" in r.text:
                    print("Page %s was blocked by Amazon. Please try using better proxies\n" % self.url)
                    return False
                else:
                    print("Page %s must have been blocked by Amazon as the status code was %d" % (
                        self.url, r.status_code))
                return None
            # Pass the HTML of the page and create
            return r.text, session
