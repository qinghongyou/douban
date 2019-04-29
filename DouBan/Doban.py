import requests
from lxml import etree
import time
import re
import pymysql
from urllib import request
from bs4 import BeautifulSoup

count=0
# 连接MYSQL数据库
db = pymysql.connect(host='127.0.0.1', user='root', password='root', db='douban', port=3306, charset='utf8')
print('连接数据库成功！')
conn = db.cursor()  # 获取指针以操作数据库
conn.execute('set names utf8')

def main():
    for i in range(0,2):
        print(i)
        url = 'https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=&start={}'.format(i * 20)
        file = requests.get(url).json()  # 这里返回json格式的页面，所以用json来解析，而不是用text
        time.sleep(2)
        for j in range(0,20):
            dict = file['data'][j]  # 分别取出文件中‘data’ 下对应的第[j]部电影的信息
            urlname = dict['url']
            get_info(urlname)
    # 关闭MySQL连接
    conn.close()
    db.close()

def get_info(url):
    global count
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://movie.douban.com/tag/',
        'Host': 'movie.douban.com'
    }
    response = requests.get(url, headers=headers)
    e = etree.HTML(response.text)
    mov_name = " "
    mov_year = " "
    mov_name_list = e.xpath('//h1/span/text()')
    if len(mov_name_list):
        # 存在值即为真
        mov_name = mov_name_list[0]# 电影名字
        mov_year = mov_name_list[1]# 电影年份
        mov_year = mov_year.replace(mov_year[0], '').replace(mov_year[len(mov_year) - 1], '')  # 把首位的括号去除

    mov_director =" "
    mov_director_list = e.xpath('//div[@class="subject clearfix"]//div[@id="info"]/span[1]/span[2]/a/text()')
    # 电影导演
    if len(mov_director_list):
        # 存在值即为真
        mov_director = mov_director_list[0]# 电影名字

    mov_screenwriter = e.xpath('//div[@class="subject clearfix"]//div[@id="info"]/span[2]/span[2]/a/text()')
    mov_screenwriter_str = ','.join(mov_screenwriter) # 电影编剧

    mov_actor = e.xpath('//div[@class="subject clearfix"]//div[@id="info"]/span[3]/span[2]/a/text()')
    mov_actor_str = ','.join(mov_actor)  # 电影主演

    mov_type = e.xpath('//div[@class="subject clearfix"]//div[@id="info"]//span[@property="v:genre"]/text()')
    mov_type_str = ','.join(mov_type)  # 电影类型

    mov_region_str = re.search(r'<span class="pl">制片国家/地区:</span>(.*?)<br/>', response.text) # 正则匹配
    mov_region = str_strip(mov_region_str[1]) if mov_region_str else ''  #制片国家/地区

    mov_language_str = re.search(r'<span class="pl">语言:</span>(.*?)<br/>', response.text)
    mov_language = str_strip(mov_language_str[1]) if mov_language_str else ''  # 语言

    mov_date = e.xpath('//div[@class="subject clearfix"]//div[@id="info"]//span[@property="v:initialReleaseDate"]/text()')
    mov_date_str = ','.join(mov_date)  # 上映日期

    mov_time = e.xpath('//div[@class="subject clearfix"]//div[@id="info"]//span[@property="v:runtime"]/text()')
    mov_time_str = ','.join(mov_time)  # 片长

    mov_score='0'
    mov_score_list = e.xpath('//strong[@class="ll rating_num"]/text()') # 豆瓣评分
    if len(mov_score_list):
        # 存在值即为真
        mov_score = mov_score_list[0]

    mov_people_remark='0'
    mov_people_remark_list = e.xpath('//div[@class="rating_sum"]/a[@class="rating_people"]/span/text()')  # 评价人数
    if len(mov_people_remark_list):
        # 存在值即为真
        mov_people_remark = mov_people_remark_list[0]

    count = count + 1
    data = [mov_name,mov_year,mov_director,mov_screenwriter_str,mov_actor_str,mov_type_str,mov_region,mov_language,mov_date_str,mov_time_str,mov_score,mov_people_remark]
    print(str(count)+"---"+','.join(data))
    # 写入MYSQL数据库
    sql = u"INSERT INTO t_douban1(mov_name," \
          u"mov_year,mov_director,mov_screenwriter_str,mov_actor_str,mov_type_str," \
          u"mov_region,mov_language,mov_date_str,mov_time_str,mov_score,mov_people_remark) " \
          u"VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    conn.execute(sql, data)
    db.commit()  # 提交操作
    # print('插入数据成功!')

def str_strip(s):
    return s.strip()

if __name__ == '__main__':
    main()