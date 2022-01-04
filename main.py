#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import urllib.request as req
from xml.dom.minidom import parseString
import json.decoder
import os.path
import shutil

from gevent import monkey

monkey.patch_all()
import gevent
from gevent.queue import Queue
from datetime import datetime
import requests
import yaml

from fire import Fire

from config import (
    SERVER_PATH_TOPIC,
    SERVER_DIR_STORAGE,
    SERVER_PATH_README,
    SERVER_PATH_DOCS,
    SERVER_PATH_STORAGE_MD,
    TIME_ZONE_CN,
    # END_DATE,
    logger
)

def getResult(search_query='all:fake+news+OR+all:rumour', start=0, max_results=5, sortBy='submittedDate', sortOrder='descending'):
    url = 'http://export.arxiv.org/api/query?search_query={}&start={}&max_results={}&sortBy={}&sortOrder={}'.format(
        search_query, start, max_results, sortBy, sortOrder
    )
    flag = True
    print("-"*10, url)
    data = req.urlopen(url)
    xml_data = data.read().decode('utf-8')
    DOMTree = parseString(xml_data)

    collection = DOMTree.documentElement
    entrys = collection.getElementsByTagName("entry")
    results = []
    # end = datetime.strptime(END_DATE,"%Y-%m-%d")
    for entry in entrys:
        paper_published_time = entry.getElementsByTagName('published')[0].childNodes[0].data
        # current = paper_published_time.split('T')[0]
        # print(paper_published_time.split('T')[0], (end - datetime.strptime(current,"%Y-%m-%d")).days)
        # if (end - datetime.strptime(current,"%Y-%m-%d")).days > 0:
        #     flag = False
        #     break
        paper_url = entry.getElementsByTagName('id')[0].childNodes[0].data
        paper_updated_time = entry.getElementsByTagName('updated')[0].childNodes[0].data
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
    return results, flag


class ToolBox:
    @staticmethod
    def log_date(mode="log"):
        if mode == "log":
            return str(datetime.now(TIME_ZONE_CN)).split(".")[0]
        elif mode == "file":
            return str(datetime.now(TIME_ZONE_CN)).split(" ")[0]

    @staticmethod
    def get_yaml_data() -> dict:
        with open(SERVER_PATH_TOPIC, "r", encoding="utf8") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
        return data

    @staticmethod
    def handle_html(url: str):
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.44"
        }
        proxies = {"http": None, "https": None}
        session = requests.session()
        response = session.get(url, headers=headers, proxies=proxies)
        try:
            data_ = response.json()
            return data_
        except json.decoder.JSONDecodeError as e:
            logger.error(e)


class CoroutineSpeedup:
    """轻量化的协程控件"""
    def __init__(
            self,
            work_q: Queue = None,
            task_docker=None,
    ):
        # 任务容器：queue
        self.worker = work_q if work_q else Queue()
        self.channel = Queue()
        # 任务容器：迭代器
        self.task_docker = task_docker
        # 协程数
        self.power = 32
        # 任务队列满载时刻长度
        self.max_queue_size = 0
        self.cache_space = []
        self.max_results = 30

    def _adaptor(self):
        while not self.worker.empty():
            task: dict = self.worker.get_nowait()
            if task.get("pending"):
                self.runtime(context=task.get("pending"))
            elif task.get("response"):
                self.parse(context=task)

    def _progress(self):
        p = self.max_queue_size - self.worker.qsize() - self.power
        p = 0 if p < 1 else p
        return p

    def runtime(self, context: dict):
        keyword_ = context.get("keyword")
        print("="*6, keyword_)
        res, _ = getResult(
            search_query=keyword_,
            max_results=self.max_results
        )
        # count = 0
        # res = []
        # while True:
        #     tmp, flag = getResult(
        #         search_query=keyword_,
        #         start=self.max_results*count,
        #         max_results=self.max_results*(count+1)
        #     )
        #     if len(tmp) > 0:
        #         res += tmp
        #     if flag:
        #         break

        context.update({"response": res, "hook": context})
        self.worker.put_nowait(context)

    def parse(self, context):
        base_url = "https://arxiv.paperswithcode.com/api/v0/papers/"
        _paper = {}
        arxiv_res = context.get("response")
        for result in arxiv_res:
            paper_id = result['paper_id']
            paper_title = result['paper_title']
            paper_summary = result['paper_summary']
            paper_url = result['paper_url']

            code_url = base_url + paper_id
            paper_first_author = result['paper_authors'][0]

            publish_time = result['paper_published_time']

            ver_pos = paper_id.find('v')
            paper_key = paper_id if ver_pos == -1 else paper_id[0:ver_pos]

            # 尝试获取仓库代码
            # ----------------------------------------------------------------------------------
            # Origin(r)
            # ----------------------------------------------------------------------------------
            # {
            #   'paper_url': 'https://',
            #   'official': {'url': 'https://github.com/nyu-wireless/mmwRobotNav'},
            #   'all_official': [{'url': 'https://github.com/nyu-wireless/mmwRobotNav'}],
            #   'unofficial_count': 0,
            #   'frameworks': [],
            #   'status': 'OK'
            # }
            # ----------------------------------------------------------------------------------
            # None(r)
            # ----------------------------------------------------------------------------------
            # {
            #   'paper_url': 'https://',
            #   'official': None,
            #   'all_official': [],
            #   'unofficial_count': 0,
            #   'frameworks': [],
            #   'status': 'OK'
            # }
            response = ToolBox.handle_html(code_url)
            official_ = response.get("official")
            repo_url = official_.get("url", "null") if official_ else "null"
            # ----------------------------------------------------------------------------------
            # 编排模型
            # ----------------------------------------------------------------------------------
            # IF repo
            #   |publish_time|paper_title|paper_first_author|[paper_id](paper_url)|`[link](url)`
            # ELSE
            #   |publish_time|paper_title|paper_first_author|[paper_id](paper_url)|`null`
            if 'https://github.com' in paper_summary and repo_url == 'null':
                code = paper_summary.split('https://github.com')[-1].replace('\n', '').replace(' ', '')
                if code.endswith("."):
                    code = code[:-1]
                repo_url = 'https://github.com' + code
            _paper.update({
                paper_key: {
                    "publish_time": publish_time,
                    "title": paper_title.replace('\n', ' '),
                    # "summary": paper_summary.replace('\n', ' '),
                    "authors": f"{paper_first_author} et.al.",
                    "id": paper_id,
                    "paper_url": paper_url,
                    "repo": repo_url
                },
            })
        self.channel.put_nowait({
            "paper": _paper,
            "topic": context["hook"]["topic"],
            "subtopic": context["hook"]["subtopic"],
            # "fields": ["Publish Date", "Title", "Summary", "Author", "PDF", "Code"]
            "fields": ["Publish Date", "Title", "Author", "PDF", "Code"]
        })
        logger.success(
            f"handle [{self.channel.qsize()}/{self.max_queue_size}]"
            f" | topic=`{context['topic']}` subtopic=`{context['hook']['subtopic']}`")

    def offload_tasks(self):
        if self.task_docker:
            for task in self.task_docker:
                self.worker.put_nowait({"pending": task})
        self.max_queue_size = self.worker.qsize()

    def overload_tasks(self):
        ot = _OverloadTasks()
        file_obj: dict = {}
        while not self.channel.empty():
            # 将上下文替换成 Markdown 语法文本
            context: dict = self.channel.get()
            md_obj: dict = ot.to_markdown(context)

            # 子主题分流
            if not file_obj.get(md_obj["hook"]):
                file_obj[md_obj["hook"]] = md_obj["hook"]
            file_obj[md_obj["hook"]] += md_obj["content"]

            # 生成 mkdocs 所需文件
            os.makedirs(os.path.join(SERVER_PATH_DOCS, f'{context["topic"]}'), exist_ok=True)
            with open(os.path.join(SERVER_PATH_DOCS, f'{context["topic"]}', f'{context["subtopic"]}.md'), 'w') as f:
                f.write(md_obj["content"])
               

        # 生成 Markdown 模板文件
        template_ = ot.generate_markdown_template(
            content="".join(list(file_obj.values())))
        # 存储 Markdown 模板文件
        ot.storage(template_, obj_="database")

        return template_

    def go(self, power: int):
        # 任务重载
        self.offload_tasks()
        # 配置弹性采集功率
        if self.max_queue_size != 0:
            self.power = self.max_queue_size if power > self.max_queue_size else power
        # 任务启动
        task_list = []
        for _ in range(self.power):
            task = gevent.spawn(self._adaptor)
            task_list.append(task)
        gevent.joinall(task_list)


class _OverloadTasks:
    def __init__(self):
        self._build()

        # yyyy-mm-dd
        self.update_time = ToolBox.log_date(mode="log")

        self.storage_path_by_date = SERVER_PATH_STORAGE_MD.format(
            ToolBox.log_date('file'))
        self.storage_path_readme = SERVER_PATH_README
        self.storage_path_docs = SERVER_PATH_DOCS

    # -------------------
    # Private API
    # -------------------
    @staticmethod
    def _build():
        if not os.path.exists(SERVER_DIR_STORAGE):
            os.mkdir(SERVER_DIR_STORAGE)

    @staticmethod
    def _set_markdown_hyperlink(text, link):
        return f"[{text}]({link})"

    def _generate_markdown_table_content(self, paper: dict):
        paper['publish_time'] = f"**{paper['publish_time']}**"
        paper['title'] = f"**{paper['title']}**"
        # paper['summary'] = f"`{paper['summary']}`"
        _pdf = self._set_markdown_hyperlink(
            text=paper['id'], link=paper['paper_url'])
        _repo = self._set_markdown_hyperlink(
            text="link", link=paper['repo']) if "http" in paper['repo'] else "null"

        line = f"|{paper['publish_time']}" \
               f"|{paper['title']}" \
               f"|{paper['authors']}" \
               f"|{_pdf}" \
               f"|{_repo}|\n"

        return line

    @staticmethod
    def _set_style_to(style: str = "center"):
        return " :---: " if style == "center" else " --- "

    # -------------------
    # Public API
    # -------------------
    def storage(self, template: str, obj_: str = "database"):
        """
        将 Markdown 模板存档
        @param template:
        @param obj_: database:将 Markdown 模板存档至 database/store 中。其他值，替换根目录下的 README
        @return:
        """
        path_factory = {
            'database': self.storage_path_by_date,
            'readme': self.storage_path_readme,
            'docs': self.storage_path_docs
        }
        if obj_ not in path_factory.keys():
            path_ = path_factory['readme']
        else:
            path_ = path_factory[obj_]
        with open(path_, "w", encoding="utf8") as f:
            for i in template:
                f.write(i)

    def generate_markdown_template(self, content: str):
        _project = f"# arxiv-daily\n"
        _pin = f" 自动更新 @ {self.update_time} Asia/Shanghai\n"

        _form = _project + _pin + content

        return _form

    def to_markdown(self, context: dict) -> dict:
        _fields = context["fields"]
        _topic = context["topic"]
        _subtopic = context["subtopic"]
        _paper_obj = context["paper"]

        _topic_md = f"\n## {_topic}\n"
        _subtopic_md = f"\n### {_subtopic}\n"
        _fields_md = f"|{'|'.join(_fields)}|\n"
        _style_md = f"|{'|'.join([self._set_style_to('center') for _ in range(len(_fields))])}|\n"
        table_lines = "".join([self._generate_markdown_table_content(
            paper) for paper in _paper_obj.values()])

        _content_md = _subtopic_md + _fields_md + _style_md + table_lines

        return {"hook": _topic_md, "content": _content_md}


class Scaffold:
    def __init__(self):
        pass

    @staticmethod
    @logger.catch()
    def run(env: str = "development", power: int = 16):
        """
        Start the test sample.

        Usage: python daily_arxiv.py run
        or: python daily_arxiv.py run --env=production  生产环境下运行

        @param power:  synergy power. The recommended value interval is [2,16].The default value is 37.
        @param env: Optional with [development production]
        @return:
        """
        # Get tasks
        context = ToolBox.get_yaml_data()

        # Set tasks
        pending_atomic = [{"subtopic": subtopic, "keyword": keyword.replace('"', ""), "topic": topic}
                          for topic, subtopics in context.items() for subtopic, keyword in subtopics.items()]

        # Offload tasks
        booster = CoroutineSpeedup(task_docker=pending_atomic)
        booster.go(power=power)

        # Overload tasks
        template_ = booster.overload_tasks()

        # Replace project README file.
        if env == "production":
            with open(SERVER_PATH_README, "w", encoding="utf8") as f:
                for i in template_:
                    f.write(i)
            
            shutil.copyfile(SERVER_PATH_README, os.path.join(SERVER_PATH_DOCS, "index.md"))

if __name__ == "__main__":
    Fire(Scaffold)
