import threading  # 多线程模块
import queue  # 队列模块
import requests
from lxml import etree
import time
import random
import json
from bs4 import BeautifulSoup
#import mysql.connector

concurrent = 50  # 采集线程数
conparse = 3  # 解析线程


class Parse(threading.Thread):  # 解析线程类
    # 初始化属性
    def __init__(self, number, data_list, req_thread, f):
        super(Parse, self).__init__()
        self.number = number  # 线程编号
        self.data_list = data_list  # 数据队列
        self.req_thread = req_thread  # 请求队列，为了判断采集线程存活状态
        self.f = f  # 获取文件对象
        self.is_parse = True  # 判断是否从数据队列里提取数据


def run(self):
    print("======")
    print('启动%d号解析线程' % self.number)
    # 无限循环，
    while True:
        # 如何判断解析线程的结束条件
        for t in self.req_thread:  # 循环所有采集线程
            if t.is_alive():  # 判断线程是否存活
                break
        else:  # 如果循环完毕，没有执行break语句，则进入else
            if self.data_list.qsize() == 0:  # 判断数据队列是否为空
                self.is_parse = False  # 设置解析为False
        # 判断是否继续解析
        if self.is_parse:  # 解析
            try:
                data = self.data_list.get(timeout=3)  # 从数据队列里提取一个数据
            except Exception as e:  # 超时以后进入异常
                data = None
            # 如果成功拿到数据，则调用解析方法
            if data is not None:
                self.parse(data)  # 调用解析方法
        else:
            break  # 结束while 无限循环
    #print('退出%d号解析线程' % self.number)


# 页面解析函数
def parse(self, data):
    html = etree.HTML(data)
    # 获取所有段子div
    duanzi_div = html.xpath('//div[@id="content-left"]/div')
    for duanzi in duanzi_div:
        # 获取昵称
        nick = duanzi.xpath('./div//h2/text()')[0]
        nick = nick.replace('\n', '')
        # 获取年龄
        age = duanzi.xpath('.//div[@class="author clearfix"]/div/text()')
        if len(age) > 0:
            age = age[0]
        else:
            age = 0
        # 获取性别
        gender = duanzi.xpath('.//div[@class="author clearfix"]/div/@class')
    if len(gender) > 0:
        if 'women' in gender[0]:
            gender = '女'
        else:
            gender = '男'
    else:
        gender = '中'
        # 获取段子内容
    content = duanzi.xpath('.//div[@class="content"]/span[1]/text()')[0].strip()
    # 获取好笑数
    good_num = duanzi.xpath('./div//span[@class="stats-vote"]/i/text()')[0]
    # 获取评论
    common_num = duanzi.xpath('./div//span[@class="stats-comments"]//i/text()')[0]
    item = {
        'nick': nick,
        'age': age,
        'gender': gender,
        'content': content,
        'good_num': good_num,
        'common_num': common_num,
    }
    self.f.write(json.dumps(item, ensure_ascii=False) + '\n')


class Crawl(threading.Thread):  # 采集线程类
    # 初始化
    def __init__(self, number, req_list, data_list):
        # 调用Thread 父类方法
        super(Crawl, self).__init__()
        # 初始化子类属性
        self.number = number
        self.req_list = req_list
        self.data_list = data_list
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36'
        }
        # 线程启动的时候调用

    def run(self):
        # 输出启动线程信息
        print('启动采集线程%d号' % self.number)
        # 如果请求队列不为空，则无限循环，从请求队列里拿请求url
        while self.req_list.qsize() > 0:
            # 从请求队列里提取url
            url = self.req_list.get()
            print('%d号线程采集：%s' % (self.number, url))

            # 防止请求频率过快，随机设置阻塞时间
            time.sleep(random.randint(1, 3))
            # 发起http请求，获取响应内容，追加到数据队列里，等待解析
            response = requests.get(url, headers=self.headers)

            response.encoding = 'utf-8'
            #print(response.text)
            soup0 = BeautifulSoup(response.text, "lxml")
            aa = soup0.find_all('div', attrs={"class": "content clearfix"})
            #print(aa)
            for item1 in aa:
                try:
                    soup1 = BeautifulSoup(str(item1), 'lxml')
                    name = soup1.find_all('span', attrs={"itemprop": "name"})
                    adress_id = soup1.find_all('span', attrs={"class": "field-label"})

                    adress = soup1.find_all('span', attrs={"itemprop": "address"})
                    telephone = soup1.find_all('span', attrs={"itemprop": "telephone"})
                    # faxNumber = soup1.find_all('span', attrs={"itemprop": "faxNumber"})
                    employee = soup1.find_all('span', attrs={"itemprop": "employee"})
                    description = soup1.find_all('span', attrs={"class": "field-item"})  # 主营产品
                    unit_email_id = soup1.find_all('span', attrs={"class": "field-label"})
                    unit_email = soup1.find_all('span', attrs={"class": "field-item"})
                    unit_wangzhi_id = soup1.find_all('span', attrs={"class": "field-label"})

                    unit_wangzhi = soup1.find_all('span', attrs={"class": "field-item"})
                    description1 = soup1.find_all('span', attrs={"itemprop": "description"})

                    unit_name = name[0].text.strip('\n').strip(" ").replace(" ", "")
                    with open('jianshi.txt', 'a', encoding="utf8") as file:
                        file.write(unit_name + "===" + url+"\n")

                    if len(unit_name) == 0:
                        print(len(unit_name))
                        pass
                    #unit_adress = adress[0].text.strip('\n').strip(" ")

                    unit_adress_id = adress_id[1].text.strip('\n').strip(" ")
                    if unit_adress_id != "地址":
                        unit_adress = "暂无相关信息！"
                    elif unit_adress_id == "地址":
                        unit_adress = adress[0].text.strip('\n').strip(" ")
                        pass
                    print(unit_adress_id)

                    unit_telephone = telephone[0].text.strip('\n').strip(" ")
                    if len(unit_telephone) == 0:
                        pass
                    # unit_faxNumber = faxNumber[0].text.strip('\n').strip(" ")
                    unit_employee = employee[0].text.strip('\n').strip(" ")
                    if len(unit_employee) == 0:
                        pass
                    unit_description = description[5].text.strip('\n').strip(" ")
                    if len(unit_description) == 0:
                        pass
                    unit_email_id = unit_email_id[9].text.strip('\n').strip(" ")
                    # print(unit_email_id+'============')

                    if unit_email_id != "电子邮箱":
                        unit_wangzhi = unit_wangzhi[9].text.strip('\n').strip(" ")
                        unit_email = "暂无相关信息！"
                        # print(unit_wangzhi+'不正常')
                        unit_email == 'null'
                    elif unit_email_id == "电子邮箱":
                        unit_wangzhi = unit_wangzhi[10].text.strip('\n').strip(" ")
                        # print(unit_wangzhi+'正常')
                        unit_email = unit_email[9].text.strip('\n').strip(" ")
                        # print(unit_email+'正常email')
                        pass
                    if len(unit_email) == 0:
                        pass
                    unit_wangzhi_id = unit_wangzhi_id[10].text.strip('\n').strip(" ")
                    print(unit_wangzhi_id + '-----------')
                    if unit_wangzhi_id != "网站网址":
                        unit_wangzhi = "暂无相关信息！"
                    else:
                        pass
                    if len(unit_wangzhi) == 0:
                        pass
                    description1 = description1[0].text.strip('\n').strip(" ").replace(" ", "")
                    if len(description1) == 0:
                        pass


                    print(unit_name + "===" + unit_adress + "===" + unit_telephone + "===" + unit_employee + "===" + unit_description + "===" + unit_email + "===" + unit_wangzhi + "===" + description1)
                    with open('aa.txt', 'a', encoding="utf8") as file:
                        file.write(unit_name + "$$$" + unit_adress + "$$$" + unit_telephone + "$$$" + unit_employee + "$$$" + unit_description + "$$$" + unit_email + "$$$" + unit_wangzhi + "$$$" + description1+"\n")

                #     conn = mysql.connector.connect(
                #         user='root',
                #         password='qazwsx123',
                #         host='172.16.17.185',
                #         port='3306',
                #         database='dg_test_linshi'
                #     )
                #     cursor = conn.cursor()
                #     cursor.execute(
                #         "INSERT INTO Foreign_trade_companies(`user_name`,`unit_adress`,`unit_telephone`,`unit_employee`,`unit_description`,`unit_email`,`unit_wangzhi`,`description1`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                #         [unit_name, unit_adress, unit_telephone, unit_employee, unit_description, unit_email,
                #          unit_wangzhi, description1])
                #     print('************** %s %s 数据保存成功 **************')
                #     conn.commit()
                #     cursor.close()
                #
                except IndexError:
                     print("异常存储:" + unit_name)
                #     conn = mysql.connector.connect(
                #         user='root',
                #         password='qazwsx123',
                #         host='172.16.17.185',
                #         port='3306',
                #         database='dg_test_linshi'
                #     )
                #     cursor = conn.cursor()
                #     cursor.execute(
                #         "INSERT INTO abnormal_target(`target`) VALUES (%s)",
                #         [unit_name])
                #     print('************** 异常！！！+++数据保存成功 **************')
                #     conn.commit()
                #     cursor.close()
                #     continue
                #     pass

def main():
    # 生成请求队列
    req_list = queue.Queue()
    # 生成数据队列 ，请求以后，响应内容放到数据队列里
    data_list = queue.Queue()
    # 创建文件对象
    f = open('duanzi.json', 'w', encoding='utf-8')
    # 循环生成多个请求url
    for i in range(53258, 1, -1):#修改
        base_url = 'https://waimao.mingluji.com/node/%d/' % i
        # 加入请求队列
        req_list.put(base_url)
    # 生成N个采集线程
    req_thread = []
    for i in range(concurrent):
        t = Crawl(i + 1, req_list, data_list)  # 创造线程
        t.start()
        req_thread.append(t)
    # 生成N个解析线程
    parse_thread = []
    for i in range(conparse):
        t = Parse(i + 1, data_list, req_thread, f)  # 创造解析线程
        t.start()
        parse_thread.append(t)
    for t in req_thread:
        t.join()
    for t in parse_thread:
        t.join()
    # 关闭文件对象
    f.close()


if __name__ == '__main__':
    main()