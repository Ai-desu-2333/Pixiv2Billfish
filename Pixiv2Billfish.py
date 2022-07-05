# -*- encoding: utf-8 -*-
'''
@File    :   pixiv2billfish.py
@Time    :   2022/07/5 15:07:30
@Author  :   Ai-Desu
@Version :   0.1.0
@Desc    :   将pixiv插画的tag信息写入到Billfish中\
    使得在Billfish中也能通过标签查找自己喜欢的作品
    参考自 @Coder-Sakura 的 pixiv2eagle
'''
import json
import re
import sqlite3
import os.path
import time
import requests
from loguru import logger
# 强制取消警告
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from thread_pool import ThreadPool, callback

# Billfish 数据库目录
# eg.:"C:\pictures\.bf\billfish.db"
DB_PATH = r"billfish.db"

# 选择使用代理链接
proxies = {'http': 'http://localhost:prot', 'https': 'http://localhost:prot'}

# 选择是否写入标签/备注
WRITE_TAG = 1
WRITE_NOTE = 1

#跳过的文件数，0为从头开始
START_FILE_NUM = 0
#处理多少文件，0为直至结束
END_FILE_NUM = 0
# 选择是否跳过已有内容的数据
SKIP = 1

# 多线程
# TAG_TOOL为标签线程，NOTE_TOOL为备注线程
TAG_TOOL = ThreadPool(8)
NOTE_TOOL = ThreadPool(8)
FOR_TOOL = ThreadPool(WRITE_TAG + WRITE_NOTE)

temp_url = "https://www.pixiv.net/ajax/illust/"
# HEADERS
headers = {
    "Host": "www.pixiv.net",
    "referer": "https://www.pixiv.net/",
    "origin": "https://accounts.pixiv.net",
    "accept-language": "zh-CN,zh;q=0.9",
    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
}

log_path = os.path.split(os.path.abspath(__file__))[0]
# 日志写入
logger.add(
    os.path.join(log_path, "{time}.log"),
    encoding="utf-8",
    enqueue=True,
)

file_id_list = []

tag_id_list = []
tag_name_list = []

tag_join_file_file_id_list = []
tag_join_file_tag_id_list = []

note_file_id_list = []
note_note_list = []

prepare_file = []
prepare_tag = []
prepare_tag_join_file = []
prepare_note_join_file = []


def baseRequest(options, method="GET", data=None, params=None, retry_num=5):
    """
    一个小型的健壮的网络请求函数
    :params options: 包括url,自定义headers,超时时间,cookie等
    :params method: 请求类型
    :params data: POST传参
    :params params: GET传参
    :params retry_num: 重试次数

    demo如下:
    demo_headers = headers.copy()
    demo_headers['referer']  = 'www.example.com'
    options ={
        "url": origin_url,
        "headers": demo_headers
    }
    baseRequest(options = options)
    """
    try:
        response = requests.request(
            method,
            options["url"],
            data=data,
            params=params,
            cookies=options.get("cookies", ""),
            headers=headers,
            verify=False,
            timeout=options.get("timeout", 5),
            proxies=proxies
        )
        response.encoding = "utf8"
        return response
    except Exception as e:
        if retry_num > 0:
            time.sleep(0.5)
            return baseRequest(options, data, params, retry_num=retry_num - 1)
        else:
            logger.info("网络请求出错 url:{}".format(options["url"]))
            return


# 从pixiv获取标签
def get_tags(pid):
    """
    从pixiv api获取pid tag
    :params pid: pixiv插画id
    :return: [tag1,tag2...] or []
    """
    resp = baseRequest(
        options={"url": f"{temp_url}{pid}"}
    )

    if not resp:
        try:
            if resp.status_code == 404:
                logger.error("Error:{}".format('该作品已被删除，或作品ID不存在。'))
                return ['Error:404']
        except Exception as e:
            return []
        return []

    json_data = json.loads(resp.text)

    if not json_data["error"]:
        tags = json_data["body"]["tags"]["tags"]
        # 加入画师名称
        tag_list = ["Artist:" + json_data["body"]["userName"]]

        for i in tags:
            if "translation" in i.keys():
                tag_list.append(i["translation"]["en"])
            tag_list.append(i["tag"])
        tag_list = list(set(tag_list))
        tag_list_r = []
        for i in tag_list:
            i = i.replace("'", "''")
            tag_list_r.append(i)
        return list(set(tag_list_r))
    else:
        return []


def get_note(pid):
    """
    从pixiv api获取pid illustTitle userName userId bookmarkCount illustComment
    :params pid: pixiv插画id
    :return: "illustTitle userName userId bookmarkCount illustComment" or “”
    """
    resp = baseRequest(
        options={"url": f"{temp_url}{pid}"}
    )
    if not resp:
        try:
            if resp.status_code == 404:
                logger.error("Error:{}".format('该作品已被删除，或作品ID不存在。'))
                return "Error:404"
        except Exception as e:
            return ""
        return ""

    json_data = json.loads(resp.text)
    if not json_data["error"]:
        # 添加标题
        note = "Title:" + json_data["body"]["illustTitle"] + "\r\n"  # 获取标题
        # 添加作者
        note += "Artist:" + json_data["body"]["userName"] + "\r\n"
        # 添加UID
        note += "UID:" + json_data["body"]["userId"] + "\r\n"
        # 添加获取收藏数
        note += "BookMark:" + str(json_data["body"]["bookmarkCount"]) + "\r\n"
        # 添加描述
        if json_data["body"]["illustComment"] != "":
            note += "Comment:\r\n" + json_data["body"]["illustComment"]
            # 替换描述中的<br /> 为 \n <a href>替换为[url]href[/url]
            note = re.sub("<br />+", "\r\n", note).replace("<a href=\"", "[url]").replace(
                "\" target=\"_blank\">", "[/url]\r\n")
            # 删除描述中其他HTML标签
            note = re.sub("<(\S*?)[^>]*>.*?|<.*? /> ", "", note)
            # 删除转跳提示链接
            note = re.sub("\[url\]/jump.php.*\[/url\]\r\n", "", note)
        else:
            note += "No Comment\r\n"
        note = re.sub("'", "''", note)
        return note
    else:
        return ""


# 处理为pid
def get_pid(name):
    """
   处理获取到的文件名
   :param name: 文件名
   :return: pid or ""
   """
    pid = ""
    # 处理非图片扩展名，防止误识别
    if name.endswith("jpg") or name.endswith("png") or name.endswith("gif") or name.endswith(
            "webp") or name.endswith("webm") or name.endswith("zip"):
        if "-" in name:
            pid = name.split("-")[0]
        elif "_" in name:
            pid = name.split("_")[0]
        else:
            pid = name.split(".")[0]

        try:
            int(pid)
        except Exception as e:
            logger.warning("Exception:{}".format(e))
            return ""
        else:
            return pid


# 检查标签是否存在
def check_tag_exist(tag_name):
    """
   检查标签是否存在
   :params name: 标签名
   :return: bf_tag.id or False
   """
    try:
        index = tag_name_list.index(tag_name)
        return tag_id_list[index]
    except Exception as e:
        return False
    return False


# 检查文件是否已经有标签
def check_file_tag_exist(file_id):
    """
   检查文件是否已经有标签
   :params file_id: 文件id bf_file.id
   :return: True or False
   """
    try:
        tag_join_file_file_id_list.index(file_id)
        return True
    except Exception as e:
        return False


# 检测文件是否已有备注
def check_note_exist(file_id):
    """
  检查文件是否已经有备注
  :params file_id: 文件id bf_file.id
  :return: True or False
  """
    try:
        id = note_file_id_list.index(file_id)
        if note_note_list[id] is not None or note_note_list[id] != "":
            return True
    except Exception as e:
        return False


class db_tool:

    def __init__(self):
        self.WRITING_DB = 0
        if os.path.isfile(DB_PATH):
            if self.connect_db():
                return
            else:
                logger.warning("链接数据库失败，请检查数据库内容")
                exit()
        else:
            logger.warning("<DB_PATH> 未找到数据库,请检查数据库路径")
            exit()

    # 链接数据库
    def connect_db(self):
        if not self.WRITING_DB:
            try:
                conn = sqlite3.connect(DB_PATH)
                conn.row_factory = sqlite3.Row
                return conn
            except Exception as e:
                logger.warning("Exception:{}".format(e))
                return False

    # 关闭连接
    def close_db(self, conn):
        try:
            conn.close()
        except Exception as e:
            time.sleep(0.3)

    # 从数据库获取文件名
    def get_file_name(self):
        """
        从数据库获取文件名 bf_file.name
        :return: bf_file["id","name"] or None
        """
        conn = self.connect_db()
        if conn:
            cursor = conn.cursor()
            try:
                if END_FILE_NUM == 0:
                    count = cursor.execute("select count(*) from bf_file").fetchone()
                    count = count[0]
                    row = cursor.execute("SELECT id , name FROM bf_file limit " + str(START_FILE_NUM) + "," + str(count)).fetchall()
                else:
                    row = cursor.execute("SELECT id , name FROM bf_file limit " + str(START_FILE_NUM) + "," + str(END_FILE_NUM)).fetchall()

            except Exception as e:
                logger.warning("Exception:{}".format(e))
                return self.get_file_name()
            self.close_db(conn)
            if row is not None:
                return row
            else:
                return None
        else:
            # logger.warning(f"Connect db failed... Try again")
            self.close_db(conn)
            time.sleep(0.3)
            return self.get_file_name()

    # 从数据库获取标签
    def get_db_tags(self):
        conn = self.connect_db()
        if conn:
            cursor = conn.cursor()
            try:
                row = cursor.execute("SELECT id,name FROM bf_tag").fetchall()
            except Exception as e:
                logger.warning("Exception:{}".format(e))
                return self.get_db_tags()
            self.close_db(conn)
            # tag存在返回tag ID，不存在返回 False
            if row:
                return row
            else:
                return []
        else:
            # logger.warning(f"Connect db failed... Try again")
            self.close_db(conn)
            time.sleep(0.3)
            return self.get_db_tags()

    # 从数据库获取文件标签
    def get_db_tag_join_file(self):
        conn = self.connect_db()
        if conn:
            cursor = conn.cursor()
            try:
                row = cursor.execute("SELECT file_id,tag_id FROM bf_tag_join_file").fetchall()
            except Exception as e:
                logger.warning("Exception:{}".format(e))
                return self.get_db_tag_join_file()
            self.close_db(conn)
            if row:
                return row
            else:
                return []
        else:
            # logger.warning(f"Connect db failed... Try again")
            self.close_db(conn)
            time.sleep(0.3)
            return self.get_db_tag_join_file()

    # 从数据库获取备注
    def get_db_note(self):
        conn = self.connect_db()
        if conn:
            cursor = conn.cursor()
            try:
                row = cursor.execute("SELECT file_id,note FROM bf_material_userdata").fetchall()
            except Exception as e:
                logger.warning("Exception:{}".format(e))
                return self.get_db_note()
            self.close_db(conn)
            if row:
                return row
            else:
                return []
        else:
            self.close_db(conn)
            time.sleep(0.3)
            return self.get_db_note()

    # 写入标签
    def write_tag_db(self, prepare_tag):
        """
        写入标签 bf_tag.id
                bf_tag.name
        :param prepare_tag: 缓存的tag
        """
        conn = self.connect_db()
        if conn and not self.WRITING_DB:
            cursor = conn.cursor()
            try:
                self.WRITING_DB = 1
                for i in prepare_tag:
                    cursor.execute("INSERT INTO bf_tag (id,name) VALUES ('" + i["id"] + "','" + i["name"] + "')")
                conn.commit()
                self.WRITING_DB = 0
                self.close_db(conn)
                return True
            except Exception as e:
                self.close_db(conn)
                self.WRITING_DB = 0
                logger.info("Exception:{}".format(e))
                return self.write_tag_db(prepare_tag)
        else:
            time.sleep(0.3)
            return self.write_tag_db(prepare_tag)

    # 写入文件标签
    def write_tag_join_file_db(self, prepare_tag_join_file):
        """
        写入标签 bf_tag_join_file.file_id
                bf_tag_join_file.tag_id
        :param prepare_tag_join_file: 缓存的文件与tag关系
        """
        conn = self.connect_db()
        # logger.info(f"write_tag_join_file_db"+ str(self.WRITING_DB))
        if conn and not self.WRITING_DB:
            cursor = conn.cursor()
            try:
                self.WRITING_DB = 1
                for i in prepare_tag_join_file:
                    cursor.execute(
                        "INSERT INTO bf_tag_join_file (file_id,tag_id) VALUES ('" + i["file_id"] + "','" + i[
                            "tag_id"] + "')")
                conn.commit()
                self.WRITING_DB = 0
                self.close_db(conn)
                return True
            except Exception as e:
                self.close_db(conn)
                self.WRITING_DB = 0
                logger.info("Exception:{}".format(e))
                return self.write_tag_join_file_db(prepare_tag_join_file)
        else:
            time.sleep(0.3)
            self.close_db(conn)
            return self.write_tag_join_file_db(prepare_tag_join_file)

    # 写入备注
    def write_note(self, prepare_note_join_file):
        """
        写入备注 bf_material_userdata.file_id
                bf_material_userdata.note
        :param prepare_note_join_file:缓存的文件备注
        """
        conn = self.connect_db()
        # logger.info(f"write_note"+ str(self.WRITING_DB))
        if conn and not self.WRITING_DB:
            try:
                self.WRITING_DB = 1
                for i in prepare_note_join_file:
                    conn.cursor().execute("INSERT INTO bf_material_userdata (file_id,note) VALUES ('" + str(
                        i["file_id"]) + "','" + i["note"] + "')")
                conn.commit()
                self.WRITING_DB = 0
                self.close_db(conn)
                return True
            except Exception as e:
                self.close_db(conn)
                self.WRITING_DB = 0
                logger.info("Exception:{}".format(e))
                return self.write_note(prepare_note_join_file)
        else:
            time.sleep(0.3)
            self.close_db(conn)
            return self.write_note(prepare_note_join_file)


class pixiv2Billfish:
    # 计数
    tag_count = 0
    # 识别成功
    tag__count = 0
    # 无法识别
    tag_un_count = 0
    # tag写入成功
    tag_success_count = 0
    # tag已写入
    tag_pass_count = 0
    # 计数
    note_count = 0
    # 识别成功
    note__count = 0
    # 无法识别
    note_un_count = 0
    # 备注写入成功
    note_success_count = 0
    # 备注已写入
    note_pass_count = 0

    def __init__(self):
        self.task_num = 0
        self.WRITING_TAG = 0
        self.WRITING = 0
        self.done_num = 0
        self.db_tool = db_tool()

        self.bf_file = self.db_tool.get_file_name()
        self.tag_row = self.db_tool.get_db_tags()
        self.tag_file_row = self.db_tool.get_db_tag_join_file()
        self.note_row = self.db_tool.get_db_note()

        if self.bf_file is None:
            logger.warning("数据库为空！")
            exit(0)

        for i in self.tag_row:
            tag_id_list.append(i["id"])
            tag_name_list.append(i["name"])
        for i in self.tag_file_row:
            tag_join_file_file_id_list.append(i["file_id"])
            tag_join_file_tag_id_list.append(i["tag_id"])
        for i in self.note_row:
            note_file_id_list.append(i["file_id"])
            note_note_list.append(i["note"])

        self.task_len = len(list(self.bf_file))

    def main(self):
        tag_flag = 0
        note_flag = 0
        if self.bf_file is not None:
            try:
                if WRITE_NOTE:
                    FOR_TOOL.put(self.thread_task_for, ('note', self.bf_file,), callback)
                    self.task_num += self.task_len
                if WRITE_TAG:
                    FOR_TOOL.put(self.thread_task_for, ('tag', self.bf_file,), callback)
                    self.task_num += self.task_len
                while True:
                    if self.done_num >= self.task_num:
                        break
                    else:
                        time.sleep(10)
                    # break
            except Exception as e:
                logger.warning("Exception:{}".format(e))
                FOR_TOOL.close()
            finally:
                FOR_TOOL.close()

            while True:
                logger.info(
                    f"<free_list> {TAG_TOOL.free_list} <max_num> {TAG_TOOL.max_num} <generate_list> {TAG_TOOL.generate_list}")
                logger.info(
                    f"<free_list> {NOTE_TOOL.free_list} <max_num> {NOTE_TOOL.max_num} <generate_list> {NOTE_TOOL.generate_list}")
                # 正常关闭线程池
                if WRITE_NOTE:
                    if NOTE_TOOL.free_list == [] and NOTE_TOOL.generate_list == []:
                        NOTE_TOOL.close()
                        logger.info(f"<当前文件总数> {self.tag_count}")
                        logger.info(f"<成功识别文件数> {self.tag__count}")
                        logger.info(f"<无法识别文件数> {self.tag_un_count}")
                        logger.info(f"<备注写入成功数> {self.note_success_count}")
                        logger.info(f"<备注跳过数> {self.note_pass_count}")
                        note_flag = 1
                else:
                    note_flag = 1
                if WRITE_TAG:
                    if TAG_TOOL.free_list == [] and TAG_TOOL.generate_list == []:
                        TAG_TOOL.close()
                        logger.info(f"<当前文件总数> {self.tag_count}")
                        logger.info(f"<成功识别文件数> {self.tag__count}")
                        logger.info(f"<无法识别文件数> {self.tag_un_count}")
                        logger.info(f"<标签写入成功数> {self.tag_success_count}")
                        logger.info(f"<标签跳过数> {self.tag_pass_count}")
                        tag_flag = 1
                else:
                    tag_flag = 1
                if tag_flag and note_flag:
                    t = 0
                    logger.info("<TOOLS was closed writing db now...>")
                    while True:
                        if WRITE_TAG:
                            if not self.WRITING and prepare_tag is not None:
                                if self.write_tag_in_db(True):
                                    logger.info("<write_tag_in_db Success>")
                                    t += 1
                            if not self.WRITING and prepare_tag_join_file is not None:
                                if self.write_tag_join_file_db(True):
                                    logger.info("<write_tag_join_file_db Success>")
                                    t += 1
                        if WRITE_NOTE:
                            if not self.WRITING:
                                if self.write_note_join_file_db(True):
                                    logger.info("<write_note_join_file_db Success>")
                                    t += 1
                        if t == WRITE_TAG * 2 + WRITE_NOTE:
                            break
                break
        else:
            logger.error("数据库中没有文件")

    @logger.catch
    def thread_task_for(self, flag, bf_file, ):
        try:
            for _ in range(0, len(list(bf_file))):
                if flag == "note":
                    NOTE_TOOL.put(self.thread_task_note, (bf_file[_], _ + 1,), callback)
                elif flag == "tag":
                    TAG_TOOL.put(self.thread_task_tag, (bf_file[_], _ + 1,), callback)
                else:
                    logger.error(f"参数错误!")
                    exit()

        except Exception as e:
            logger.warning("Exception:{}".format(e))
            if flag == "note":
                NOTE_TOOL.close()
            if flag == "tag":
                TAG_TOOL.close()

        finally:
            if flag == "note":
                NOTE_TOOL.close()
            if flag == "tag":
                TAG_TOOL.close()

    @logger.catch
    # 写入标签线程
    def thread_task_tag(self, _, num, ):
        """
        线程任务函数
        :params _: 文件列表
        :params num: 当前序号
        """
        # 文件在bf_file 中的id，name
        file_id = _["id"]
        name = _["name"]

        logger.info(f"<{num}/{self.task_len}> <name> {name} <Start>")
        pid = get_pid(name)
        # 成功识别
        if pid:
            self.tag__count += 1
        # 无法识别
        else:
            self.tag_count += 1
            self.tag_un_count += 1
            logger.info(f"<{num}/{self.task_len}> <name> {name} <un_count>")
            self.done_num += 1
            return

        # 写入标签
        if check_file_tag_exist(file_id) and SKIP:
            self.tag_count += 1
            self.tag_pass_count += 1
            logger.info(f"<{num}/{self.task_len}> <name> {name} <Skip>")
            self.done_num += 1
            return
        else:
            tag_list = get_tags(pid)

            if not tag_list:
                self.tag_count += 1
                self.tag_pass_count += 1
                self.done_num += 1
                return

        # 写入
        if tag_list:
            self.write_tag_list(file_id, tag_list)
            self.write_tag_in_db(False)
            self.write_tag_join_file_db(False)
            self.tag_success_count += 1
            logger.info(f"<{num}/{self.task_len}> <name> {name} <Written>")
        self.done_num += 1
        self.tag_count += 1

    # 写入备注线程
    @logger.catch
    def thread_task_note(self, _, num, ):
        """
        线程任务函数
        :params _: 文件列表
        :params num: 当前序号
        """
        # 文件在bf_file 中的id，name
        id = _["id"]
        name = _["name"]

        logger.info(f"<{num}/{self.task_len}> <name> {name} <Start>")
        pid = get_pid(name)
        # 成功识别
        if pid:
            self.note__count += 1
        # 无法识别
        else:
            self.note_count += 1
            self.note_un_count += 1
            logger.info(f"<{num}/{self.task_len}> <name> {name} <un_count>")
            self.done_num += 1
            return

        # 写入标签
        if check_note_exist(id) and SKIP:
            self.note_count += 1
            self.note_pass_count += 1
            logger.info(f"<{num}/{self.task_len}> <name> {name} <Skip>")
            self.done_num += 1
            return
        else:
            note = get_note(pid)

            if note == "":
                self.note_count += 1
                self.note_pass_count += 1
                self.done_num += 1
                return

        # 写入
        if note:
            self.write_note_list(id, note)
            self.write_note_join_file_db(False)
            self.note_success_count += 1
        time.sleep(0.1)
        logger.info(f"<{num}/{self.task_len}> <name> {name} <Written>")
        self.done_num += 1
        self.note_count += 1

    def write_tag_list(self, file_id, tag_list):
        """
        写入临时标签，'prepare_tag'
        :params file_id: file_id
        :params tag_list: 将要写入的tag列表
        """
        for i in tag_list:
            tag_id = check_tag_exist(i)
            if tag_id:
                prepare_tag_join_file.append({'file_id': str(file_id), 'tag_id': str(tag_id)})

            else:
                if not self.WRITING_TAG:
                    self.WRITING_TAG = 1
                    if tag_id_list:
                        tag_id = int(tag_id_list[len(tag_id_list) - 1]) + 1
                    else:
                        tag_id = 1
                    prepare_tag.append({"id": str(tag_id), "name": str(i)})
                    prepare_tag_join_file.append({"file_id": str(file_id), "tag_id": str(tag_id)})
                    tag_id_list.append(tag_id)
                    tag_name_list.append(i)
                    self.WRITING_TAG = 0
                else:
                    time.sleep(0.3)
                    self.write_tag_list(file_id, tag_list)

    def write_note_list(self, file_id, note):
        """
        写入临时备注，'prepare_note_join_file'
        :params file_id: file_id
        :params note: 将要写入的备注
        """
        prepare_note_join_file.append({"file_id": file_id, "note": note})

    def write_tag_in_db(self, flag):
        if (len(prepare_tag) >= 20 or flag) and not self.WRITING:
            self.WRITING = 1
            if self.db_tool.write_tag_db(prepare_tag):
                self.WRITING = 0
                prepare_tag.clear()
                return True
        else:
            return False

    def write_tag_join_file_db(self, flag):
        if (len(prepare_tag_join_file) >= 50 or flag) and not self.WRITING:
            self.WRITING = 1
            if self.db_tool.write_tag_join_file_db(prepare_tag_join_file):
                self.WRITING = 0
                prepare_tag_join_file.clear()
                return True
        else:
            return False

    def write_note_join_file_db(self, flag):
        if (len(prepare_note_join_file) >= 10 or flag) and not self.WRITING:
            self.WRITING = 1
            if self.db_tool.write_note(prepare_note_join_file):
                self.WRITING = 0
                prepare_note_join_file.clear()
                return True
        else:
            return False


if __name__ == '__main__':
    test = pixiv2Billfish()
    test.main()
