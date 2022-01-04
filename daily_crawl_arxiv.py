#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import urllib.request as libreq
from xml.dom.minidom import parseString
import datetime
import requests
import json
import os
import shutil
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

# paperWithCode API
base_url = "https://arxiv.paperswithcode.com/api/v0/papers/"

def get_authors(authors, first_author=False):
    output = str()
    if first_author == False:
        output = ", ".join(str(author) for author in authors)
    else:
        output = authors[0]
    return output

def get_categories(categories):
    return ", ".join(str(category) for category in categories)

def sort_papers(papers):
    output = dict()
    keys = list(papers.keys())
    keys.sort(reverse=True)
    for key in keys:
        output[key] = papers[key]
    return output

def get_yaml_data(yaml_file: str):
    fs = open(yaml_file)
    data = yaml.load(fs, Loader=Loader)
    print(data)
    return data

def getResult(search_query='all:fake+news+OR+all:rumour', start=0, max_results=5, sortBy='submittedDate', sortOrder='descending'):
    url = 'http://export.arxiv.org/api/query?search_query={}&start={}&max_results={}&sortBy={}&sortOrder={}'.format(
        search_query, start, max_results, sortBy, sortOrder
    )
    print(url)
    data = libreq.urlopen(url)
    xml_data = data.read()
    DOMTree = parseString(xml_data)

    collection = DOMTree.documentElement
    entrys = collection.getElementsByTagName("entry")
    results = []
    for entry in entrys:
        paper_url = entry.getElementsByTagName('id')[0].childNodes[0].data
        paper_updated_time = entry.getElementsByTagName('updated')[0].childNodes[0].data
        paper_published_time = entry.getElementsByTagName('published')[0].childNodes[0].data
        paper_title = entry.getElementsByTagName('title')[0].childNodes[0].data
        paper_summary = entry.getElementsByTagName('summary')[0].childNodes[0].data
        authors = entry.getElementsByTagName('author')
        paper_authors = []
        for author in authors:
            paper_authors.append(author.getElementsByTagName('name')[0].childNodes[0].data)
        paper_journal = 'null'
        if len(entry.getElementsByTagName('arxiv:journal_ref')) > 0:
            paper_journal = entry.getElementsByTagName('arxiv:journal_ref')[0].childNodes[0].data
        paper_primary_category = entry.getElementsByTagName('arxiv:primary_category')[0].attributes["term"].nodeValue
        categories = entry.getElementsByTagName('category')
        paper_categories = []
        for category in categories:
            paper_categories.append(category.attributes["term"].nodeValue)
        results.append({
            'paper_id': paper_url.split('arxiv.org/abs/')[-1],
            'paper_url': paper_url,
            'paper_pdf_url': paper_url.replace('/abs/', '/pdf/'),
            'paper_updated_time': paper_updated_time.replace('T', ' ').replace('Z', ''),
            'paper_published_time': paper_published_time.replace('T', ' ').replace('Z', ''),
            'paper_title': paper_title,
            'paper_summary': paper_summary,
            'paper_authors': paper_authors,
            'paper_journal': paper_journal,
            'paper_primary_category': paper_primary_category,
            'paper_categories': paper_categories,
        })
    return results

def get_daily_papers(topic: str, query: str = "fake news", max_results=2):
    """
    @param topic: str
    @param query: str
    @return paper_with_code: dict
    """
    # output
    content = dict()

    # content
    output = dict()

    cnt = 0

    for result in getResult(search_query=query, max_results=max_results):
        paper_url      = result.paper_url
        paper_id       = result.paper_id
        # update_time = result.paper_updated_time
        publish_time = result.paper_published_time
        paper_title    = result.paper_title
        # paper_authors  = get_authors(result.authors)
        paper_first_author = get_authors(result.paper_authors, first_author=True)
        # paper_abstract = result.summary.replace("\n"," ")
        # paper_comment = result.comment
        # paper_journal_ref = result.journal_ref
        # paper_doi = result.doi
        # primary_category = result.primary_category
        # paper_categories = get_categories(result.categories)
        
        # paper_links = result.links
        # paper_pdf_url = result.pdf_url
        code_url       = base_url + paper_id # paperWithCode

        print("Time = ", publish_time,
              " title = ", paper_title,
              " author = ", paper_first_author)

        # eg: 2108.09112v1 -> 2108.09112
        ver_pos = paper_id.find('v')
        if ver_pos == -1:
            paper_key = paper_id
        else:
            paper_key = paper_id[0: ver_pos]

        try:
            r = requests.get(code_url).json()
            # source code link
            if "official" in r and r["official"]:
                cnt += 1
                repo_url = r["official"]["url"]
                content[paper_key] = f"|**{publish_time}**|**{paper_title}**|{paper_first_author} et.al.|[{paper_id}]({paper_url})|**[link]({repo_url})**|\n"
            else:
                content[paper_key] = f"|**{publish_time}**|**{paper_title}**|{paper_first_author} et.al.|[{paper_id}]({paper_url})|null|\n"

        except Exception as e:
            print(f"exception: {e} with id: {paper_key}")

    output = { topic: content }
    return output


def update_json_file(filename, data):
    with open(filename, "r") as f:
        content = f.read()
        if not content:
            m = {}
        else:
            m = json.loads(content)

    json_data = m.copy()

    # update papers in each keywords
    for topic in data.keys():
        if not topic in json_data.keys():
            json_data[topic] = {}
        for subtopic in data[topic].keys():
            papers = data[topic][subtopic]

            if subtopic in json_data[topic].keys():
                json_data[topic][subtopic].update(papers)
            else:
                json_data[topic][subtopic] = papers

    with open(filename, "w") as f:
        json.dump(json_data, f)


def json_to_md(filename, to_web=False):
    """
    @param filename: str
    @return None
    """

    DateNow = datetime.date.today()
    DateNow = str(DateNow)
    DateNow = DateNow.replace('-', '.')

    with open(filename, "r") as f:
        content = f.read()
        if not content:
            data = {}
        else:
            data = json.loads(content)

    if to_web == False:
        md_filename = "README.md"
        # clean README.md if daily already exist else create it
        with open(md_filename, "w+") as f:
            pass

        # write data into README.md
        with open(md_filename, "a+") as f:

            f.write("## 更新日期： " + DateNow + "\n\n")
            for topic in data.keys():
                f.write("## " + topic + "\n\n")
                for subtopic in data[topic].keys():
                    day_content = data[topic][subtopic]
                    if not day_content:
                        continue
                    # the head of each part
                    f.write(f"### {subtopic}\n\n")

                    f.write("| 发布日期 | 标题 | 作者 | PDF | 代码 |\n" +
                            "|---|---|---|---|---|\n")

                    # sort papers by date
                    day_content = sort_papers(day_content)

                    for _, v in day_content.items():
                        if v is not None:
                            f.write(v)

                    f.write(f"\n")
    else:
        if os.path.exists('docs'):
            shutil.rmtree('docs')
        if not os.path.isdir('docs'):
            os.mkdir('docs')

        shutil.copyfile('README.md', os.path.join('docs', 'index.md'))

        for topic in data.keys():
            os.makedirs(os.path.join('docs', topic), exist_ok=True)
            md_indexname = os.path.join('docs', topic, "index.md")
            with open(md_indexname, "w+") as f:
                f.write(f"# {topic}\n\n")

            # print(f'web {topic}')

            for subtopic in data[topic].keys():
                md_filename = os.path.join('docs', topic, f"{subtopic}.md")
                # print(f'web {subtopic}')

                # clean README.md if daily already exist else create it
                with open(md_filename, "w+") as f:
                    pass

                with open(md_filename, "a+") as f:
                    day_content = data[topic][subtopic]
                    if not day_content:
                        continue
                    # the head of each part
                    f.write(f"# {subtopic}\n\n")
                    f.write("| 发布日期 | 标题 | 作者 | PDF | 代码 |\n")
                    f.write(
                        "|:---------|:-----------------------|:---------|:------|:------|\n")

                    # sort papers by date
                    day_content = sort_papers(day_content)

                    for _, v in day_content.items():
                        if v is not None:
                            f.write(v)

                    f.write(f"\n")

                with open(md_indexname, "a+") as f:
                    day_content = data[topic][subtopic]
                    if not day_content:
                        continue
                    # the head of each part
                    f.write(f"## {subtopic}\n\n")
                    f.write("| 发布日期 | 标题 | 作者 | PDF | 代码 |\n")
                    f.write(
                        "|:---------|:-----------------------|:---------|:------|:------|\n")

                    # sort papers by date
                    day_content = sort_papers(day_content)

                    for _, v in day_content.items():
                        if v is not None:
                            f.write(v)

                    f.write(f"\n")

    print("finished")


if __name__ == "__main__":

    data_collector = dict()

    yaml_path = os.path.join(".", "topic.yml")
    yaml_data = get_yaml_data("./history/topic.yml")

    # print(yaml_data)

    keywords = dict(yaml_data)

    for topic in keywords.keys():
        for subtopic, keyword in dict(keywords[topic]).items():

            # topic = keyword.replace("\"","")
            print("Keyword: " + subtopic)
            try:
                data = get_daily_papers(
                    subtopic, query=keyword, max_results=10)
            except Exception as e:
                print(e)
                print(f'CANNOT get {subtopic} data from arxiv')
                data = None
            # time.sleep(random.randint(2, 10))

            if not topic in data_collector.keys():
                data_collector[topic] = {}

            if data:
                data_collector[topic].update(data)

            print(data)
            # print(data_collector)

            print("\n")

    print(data_collector)
    # update README.md file
    json_file = "arxiv-daily.json"
#     if ~os.path.exists(json_file):
#         with open(json_file,'w')as a:
#             print("create " + json_file)

    # update json data
    update_json_file(json_file, data_collector)
    # json data to markdown
    json_to_md(json_file)

    # json data to markdown
    json_to_md(json_file, to_web=True)
