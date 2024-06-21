# Real Python, Fake Jobs

Scripts description:
1. parseFiles - parser/scrapper for html-files in described location. Searches for specific tags with content. As result creates result.pydantic table of pages content.

2. parseWebPage - similar parser/scrapper based on general url to realpython webpage, where grabs all href's for jobs and parse those web pages for content in specific tags. Result in pydantic table.

3. read_result - script to open result table file and print content of grabbed from pages data.
