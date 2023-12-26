from bs4 import BeautifulSoup
import requests,json,time
import pandas as pd

def run():

    url_base = 'https://bj.ke.com/ershoufang/'  # 基本链接
    url_place = 'haidian'  # 查询地点
    url_para = 'sf1y3l1l2l3/'  # 参数配置
    # 查询参数对应的内容：
    # sf1:普通住宅
    # y1:5年以内,  y2:10年以内,   y3:15年以内,   y4:20年以内
    # l1:1室,     l2:2室,       l3:3室
    # lc1:低楼层,  lc2:中楼层,   lc3:高楼层
    total_page = find_total_page_count(url_base+url_place)
    for current in range(1,total_page + 1):
        url = url_base + url_place + '/pg' + str(current) + '/' + url_para
        response_data = requests.get(url=url,headers=headers).text
        bs = BeautifulSoup(response_data,'lxml')
        div_info_list = bs.find_all('div',class_='info clear')
        for div_info in div_info_list:
            house_detail_url = div_info.find('div', class_="title").a.get('href')
            area_detail_url = div_info.find('div',class_='positionInfo').a.get('href')
            # print('房源地址：',house_detail_url)
            # print('小区地址：',area_detail_url)
            area_id_list = area_detail_url.split('/')
            if area_id_list[len(area_id_list) - 1] == '':
                area_id = area_id_list[len(area_id_list) - 2]
            else:
                area_id = area_id_list[len(area_id_list) - 1]
            if area_id not in area_id_set:
                area_id_set.add(area_id)
                slove_area(area_detail_url)

            slove_house(house_detail_url)



def slove_house(house_detail_url):
    house_data = requests.get(url=house_detail_url, headers=headers).content.decode('utf-8')
    bs = BeautifulSoup(house_data, 'lxml')
    house_list = bs.find('div',class_="introContent").find_all('li')
    house_dict={}

    # 处理其他信息
    script_tags = bs.find_all('script')
    for script_tag in script_tags:
        if 'window.GLOBAL_INFOS' in script_tag.text:
            script_tag_text = script_tag.text.replace("window.GLOBAL_INFOS =", "").split("\n")
            for script_t in script_tag_text:
                script_t = script_t.replace("'", "").replace(',','')
                if 'images:' in script_t or 'agentList:' in script_t:
                    continue
                if 'houseId' in script_t:
                    house_dict['房屋记录ID'] = script_t.split(":")[1]
                    continue
                if 'title' in script_t:
                    house_dict['标题'] = script_t.split(":")[1]
                    continue
                if 'resblockId' in script_t:
                    house_dict['小区记录ID'] = script_t.split(":")[1]
                    continue
                if 'resblockName' in script_t:
                    house_dict['小区名称'] = script_t.split(":")[1]
                    continue

    house_price = bs.find('div', class_='price-container').find('div', class_='price').get_text().replace(' ','').replace('\n','')
    house_dict['房屋总价'] = house_price.split('万')[0]
    house_dict['房屋总价(单位)'] = '万'
    house_dict['房屋单价'] = house_price.split('万')[1].split('元/平米')[0]
    house_dict['房屋单价(单位)'] = "元/平米"

    area_href_list = bs.find('div', class_="areaName").find('span', class_='info').find_all('a')
    house_dict['城区'] = area_href_list[0].get_text()
    house_dict['街道/乡镇'] = area_href_list[1].get_text()

    for house_li in house_list:
        house_d = house_li.get_text().replace(' ','').replace('\n','')
        if '房屋户型' in house_d:
            house_dict['房屋户型'] = house_d.split('房屋户型')[1]
            continue
        elif '建筑面积' in house_d:
            house_dict['建筑面积'] = house_d.split('建筑面积')[1].replace('㎡','')
            continue
        elif '户型结构' in house_d:
            house_dict['户型结构'] = house_d.split('户型结构')[1]
            continue
        elif '建筑类型' in house_d:
            house_dict['建筑类型'] = house_d.split('建筑类型')[1]
            continue
        elif '所在楼层' in house_d:
            house_dict['所在楼层'] = house_d.split('所在楼层')[1].replace('咨询楼层','')
            continue
        elif '套内面积' in house_d:
            house_dict['套内面积'] = house_d.split('套内面积')[1].replace('咨询套内面积','')
            continue
        elif '房屋朝向' in house_d:
            house_dict['房屋朝向'] = house_d.split('房屋朝向')[1]
            continue
        elif '建筑结构' in house_d:
            house_dict['建筑结构'] = house_d.split('建筑结构')[1]
            continue
        elif '装修情况' in house_d:
            house_dict['装修情况'] = house_d.split('装修情况')[1]
            continue
        elif '梯户比例' in house_d:
            house_dict['梯户比例'] = house_d.split('梯户比例')[1]
            continue
        elif '供暖方式' in house_d:
            house_dict['供暖方式'] = house_d.split('供暖方式')[1]
            continue
        elif '挂牌时间' in house_d:
            house_dict['挂牌时间'] = house_d.split('挂牌时间')[1]
            continue
        elif '交易权属' in house_d:
            house_dict['交易权属'] = house_d.split('交易权属')[1]
            continue
        elif '上次交易' in house_d:
            house_dict['上次交易'] = house_d.split('上次交易')[1]
            continue
        elif '房屋用途' in house_d:
            house_dict['房屋用途'] = house_d.split('房屋用途')[1]
            continue
        elif '房屋年限' in house_d:
            house_dict['房屋年限'] = house_d.split('房屋年限')[1]
            continue
        elif '产权所属' in house_d:
            house_dict['产权所属'] = house_d.split('产权所属')[1]
            continue
        elif '抵押信息' in house_d:
            house_dict['抵押信息'] = house_d.split('抵押信息')[1]
            continue
    house_data_list.append(house_dict)


def slove_area(area_detail_url):
    area_data = requests.get(url=area_detail_url, headers=headers).content.decode('utf-8')
    bs = BeautifulSoup(area_data,'lxml')
    xiaoquUnitPrice = bs.find('div', class_='xiaoquPrice clear').find('span', class_='xiaoquUnitPrice').text
    area_data_list_t = bs.find('div', class_="xiaoquInfo").find_all('div',class_="xiaoquInfoItem")

    area_dict = {}

    script_tags = bs.find_all('script')
    for script_tag in script_tags:
        if 'window.GLOBAL_INFOS' in script_tag.text:
            script_tag_text = script_tag.text.replace("window.GLOBAL_INFOS =", "").split("\n")
            for script_t in script_tag_text:
                script_t = script_t.replace("'",'').replace(',','')
                if 'others' in script_t:
                    continue
                elif 'id:' in script_t:
                    area_dict['小区记录ID'] = script_t.split(":")[1]
                    continue
                elif 'resblockName' in script_t:
                    area_dict['小区名称'] = script_t.split(":")[1]
                    continue
                elif 'cityId' in script_t:
                    area_dict['城市ID'] = script_t.split(":")[1]
                    continue
                elif 'resblockPosition' in script_t:
                    area_dict['小区定位'] = script_t.split(":")[1]
                    continue

    area_dict['小区均价'] = xiaoquUnitPrice

    for area_data in area_data_list_t:
        area_data_d = area_data.text.replace(' ','').replace('\n','')
        if '建筑类型' in area_data_d:
            area_dict['建筑类型'] = area_data_d.split('建筑类型')[1]
            continue
        elif '房屋总数' in area_data_d:
            area_dict['房屋总数'] = area_data_d.split('房屋总数')[1]
            continue
        elif '楼栋总数' in area_data_d:
            area_dict['楼栋总数'] = area_data_d.split('楼栋总数')[1]
            continue
        elif '绿化率' in area_data_d:
            area_dict['绿化率'] = area_data_d.split('绿化率')[1]
            continue
        elif '容积率' in area_data_d:
            area_dict['容积率'] = area_data_d.split('容积率')[1]
            continue
        elif '建成年代' in area_data_d:
            area_dict['建成年代'] = area_data_d.split('建成年代')[1]
            continue
        elif '供暖类型' in area_data_d:
            area_dict['供暖类型'] = area_data_d.split('供暖类型')[1]
            continue
        elif '用水类型' in area_data_d:
            area_dict['用水类型'] = area_data_d.split('用水类型')[1]
            continue
        elif '用电类型' in area_data_d:
            area_dict['用电类型'] = area_data_d.split('用电类型')[1]
            continue
        elif '物业费' in area_data_d:
            area_dict['物业费'] = area_data_d.split('物业费')[1]
            continue
        elif '物业公司' in area_data_d:
            area_dict['物业公司'] = area_data_d.split('物业公司')[1]
            continue
        elif '开发商' in area_data_d:
            area_dict['开发商'] = area_data_d.split('开发商')[1]
            continue

    area_data_list.append(area_dict)


def find_total_page_count(url):
    response = requests.get(url=url, headers=headers)
    bs = BeautifulSoup(response.text, 'lxml')
    total_page = json.loads(bs.find(name="div", class_="page-box house-lst-page-box").get('page-data'))['totalPage']
    return total_page

if __name__ == '__main__':
    # 请求头
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
        'Origin': "https://bj.ke.com/",
        'Referer': "https://bj.ke.com/",
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/91.0.4472.106 '
                      'Safari/537.36',
    }

    area_id_set= set()
    house_data_list = []
    area_data_list = []
    run()

    if len(house_data_list) != 0:
        # 保存到本地
        df = pd.DataFrame.from_records(house_data_list)
        order = ['房屋记录ID', '标题', '小区记录ID', '小区名称','城区','街道/乡镇', '房屋总价', '房屋总价(单位)', '房屋单价',
                 '房屋单价(单位)', '房屋户型',
                 '建筑面积',
                 '户型结构', '建筑类型', '所在楼层', '套内面积', '房屋朝向', '建筑结构', '装修情况', '梯户比例',
                 '供暖方式',
                 '挂牌时间', '交易权属', '上次交易', '房屋用途', '房屋年限', '产权所属', '抵押信息']
        df = df[order]
        df.to_excel('./{}二手房源-{}.xlsx'.format('haidian', time.strftime('%Y-%m-%d', time.localtime())), index=False)

    if len(area_data_list) != 0:
        df = pd.DataFrame.from_records(area_data_list)
        order = ['小区记录ID', '小区名称', '城市ID', '小区定位', '小区均价', '建筑类型', '房屋总数',
                 '楼栋总数',
                 '绿化率',
                 '容积率', '建成年代', '供暖类型', '用水类型', '用电类型', '物业费', '物业公司', '开发商']
        df = df[order]
        df.to_excel('./{}二手房源-{}.xlsx'.format('haidian_area', time.strftime('%Y-%m-%d', time.localtime())), index=False)
