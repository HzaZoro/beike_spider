from bs4 import BeautifulSoup
import requests,json,time,threading
import pandas as pd

def run():

    url_base = 'https://bj.ke.com/ershoufang/'  # 基本链接
    # url_place = 'haidian'  # 查询地点
    url_para = 'sf1y3l1l2l3/'  # 参数配置
    # 查询参数对应的内容：
    # sf1:普通住宅
    # y1:5年以内,  y2:10年以内,   y3:15年以内,   y4:20年以内
    # l1:1室,     l2:2室,       l3:3室
    # lc1:低楼层,  lc2:中楼层,   lc3:高楼层

    thread_list = []
    city_key = city_dict.keys()
    for city_k in city_key:
        city_url_key = city_dict.get(city_k).keys()
        thread_count = int(len(city_url_key) / 4 + 1)
        print('【'+city_k+'】创建 ['+str(thread_count)+'] 个线程处理数据' )
        for thread_c in range(thread_count):
            t = threading.Thread(target=threadRun,name=(city_k + str(thread_c)),args=(city_k,city_url_key,url_base,url_para))
            thread_list.append(t)
            t.start()

    t_flag = False
    while(not t_flag):
        for t in thread_list:
            if (not t.is_alive()):
                print("线程【"+t.name+"】已处理完成")
                t_flag = True
                continue
            t_flag = False
            print("线程【" + t.name + "】还未处理完成")
        time.sleep(5)
    print("所有线程数据处理完成")



def threadRun(city_k,city_url_key,url_base,url_para):
    for url_place_key in city_url_key:
        url_place = city_dict.get(city_k).get(url_place_key)
        if url_place in sub_city_set:
            print("已处理过【" + url_place_key + "】数据，跳过该次处理")
            continue
        total_page = find_total_page_count(url_base + url_place + '/' + url_para)
        print("正在处理【", url_place_key, "】信息，共有[", total_page, "]页数据")
        sub_city_set.add(url_place)
        if total_page == 0:
            print("【" + url_place_key + "】未发现数据，跳过该次处理")
            continue
        for current in range(1, total_page + 1):
            url = url_base + url_place + '/pg' + str(current) + '/' + url_para
            response_data = requests.get(url=url, headers=headers).text
            bs = BeautifulSoup(response_data, 'lxml')
            div_info_list = bs.find_all('div', class_='info clear')
            for div_info in div_info_list:
                house_detail_url = div_info.find('div', class_="title").a.get('href')
                area_detail_url = div_info.find('div', class_='positionInfo').a.get('href')
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
    total_page = 0
    response = requests.get(url=url, headers=headers)
    bs = BeautifulSoup(response.text, 'lxml')
    if bs.find(name="div", class_="page-box house-lst-page-box") is not None :
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

    city_dict = {
        'haidian' : {
            # '安宁庄':'anningzhuang1',
            # '奥林匹克公园':'aolinpikegongyuan11',
            # '白石桥':'baishiqiao1',
            # '北太平庄':'beitaipingzhuang',
            # '厂洼':'changwa',
            # '定慧寺':'dinghuisi',
            # '二里庄':'erlizhuang',
            # '甘家口':'ganjiakou',
            # '公主坟':'gongzhufen',
            # '海淀北部新区':'haidianbeibuxinqu1',
            # '海淀其它':'haidianqita1',
            # '军博':'junbo1',
            # '六里桥':'liuliqiao1',
            # '马甸':'madian1',
            # '马连洼':'malianwa',
            # '牡丹园':'mudanyuan',
            # '清河':'qinghe11',
            # '上地':'shangdi1',
            # '世纪城':'shijicheng',
            # '双榆树':'shuangyushu',
            # '四季青':'sijiqing',
            # '苏州桥':'suzhouqiao',
            # '田村':'tiancun1',
            # '万柳':'wanliu',
            # '万寿路':'wanshoulu1',
            # '魏公村':'weigongcun',
            # '五道口':'wudaokou',
            # '五棵松':'wukesong1',
            # '小西天':'xiaoxitian1',
            # '西北旺':'xibeiwang',
            # '西二旗':'xierqi1',
            # '新街口':'xinjiekou2',
            # '西三旗':'xisanqi1',
            # '西山':'xishan21',
            # '西直门':'xizhimen1',
            # '学院路':'xueyuanlu1',
            # '颐和园':'yiheyuan',
            # '圆明园':'yuanmingyuan',
            # '玉泉路':'yuquanlu11',
            # '皂君庙':'zaojunmiao',
            # '知春路':'zhichunlu',
            # '中关村':'zhongguancun',
            '紫竹桥':'zizhuqiao'
        },
        # 'chaoyang':{
        #     # '安定门': 'andingmen',
        #     # '安贞': 'anzhen1',
        #     # '奥林匹克公园': 'aolinpikegongyuan11',
        #     # '百子湾': 'baiziwan',
        #     # '北工大': 'beigongda',
        #     # '北苑': 'beiyuan2',
        #     # 'CBD': 'cbd',
        #     # '常营': 'changying',
        #     # '朝青': 'chaoqing',
        #     # '朝阳公园': 'chaoyanggongyuan',
        #     # '朝阳门内': 'chaoyangmennei1',
        #     # '朝阳门外': 'chaoyangmenwai1',
        #     # '成寿寺': 'chengshousi1',
        #     # '大山子': 'dashanzi',
        #     # '大望路': 'dawanglu',
        #     # '定福庄': 'dingfuzhuang',
        #     # '东坝': 'dongba',
        #     # '东大桥': 'dongdaqiao',
        #     # '东直门': 'dongzhimen',
        #     # '豆各庄': 'dougezhuang',
        #     # '方庄': 'fangzhuang1',
        #     # '垡头': 'fatou',
        #     # '甘露园': 'ganluyuan',
        #     # '高碑店': 'gaobeidian',
        #     # '工体': 'gongti',
        #     # '广渠门': 'guangqumen',
        #     # '管庄': 'guanzhuang',
        #     # '国展': 'guozhan1',
        #     # '和平里': 'hepingli',
        #     # '红庙': 'hongmiao',
        #     # '欢乐谷': 'huanlegu',
        #     # '华威桥': 'huaweiqiao',
        #     # '惠新西街': 'huixinxijie',
        #     # '建国门外': 'jianguomenwai',
        #     # '健翔桥': 'jianxiangqiao1',
        #     # '劲松': 'jinsong',
        #     # '酒仙桥': 'jiuxianqiao',
        #     # '亮马桥': 'liangmaqiao',
        #     # '立水桥': 'lishuiqiao1',
        #     # '马甸': 'madian1',
        #     # '牡丹园': 'mudanyuan',
        #     # '南沙滩': 'nanshatan1',
        #     # '农展馆': 'nongzhanguan',
        #     # '潘家园': 'panjiayuan1',
        #     # '三里屯': 'sanlitun',
        #     # '三元桥': 'sanyuanqiao',
        #     # '芍药居': 'shaoyaoju',
        #     # '十八里店': 'shibalidian1',
        #     # '石佛营': 'shifoying',
        #     # '十里堡': 'shilibao',
        #     # '十里河': 'shilihe',
        #     # '首都机场': 'shoudoujichang1',
        #     # '双井': 'shuangjing',
        #     # '双桥': 'shuangqiao',
        #     # '四惠': 'sihui',
        #     # '宋家庄': 'songjiazhuang',
        #     # '太阳宫': 'taiyanggong',
        #     # '甜水园': 'tianshuiyuan',
        #     # '通州北苑': 'tongzhoubeiyuan',
        #     # '团结湖': 'tuanjiehu',
        #     # '望京': 'wangjing',
        #     # '小红门': 'xiaohongmen',
        #     # '西坝河': 'xibahe',
        #     # '燕莎': 'yansha1',
        #     # '亚运村': 'yayuncun',
        #     # '亚运村小营': 'yayuncunxiaoying',
        #     # '朝阳其它': 'zhaoyangqita',
        #     '中央别墅区': 'zhongyangbieshuqu1'
        # },
        # 'dongcheng':{
        #     # '安定门': 'andingmen',
        #     # '安贞': 'anzhen1',
        #     # '朝阳门内': 'chaoyangmennei1',
        #     # '朝阳门外': 'chaoyangmenwai1',
        #     # '崇文门': 'chongwenmen',
        #     # '灯市口': 'dengshikou',
        #     # '地安门': 'dianmen',
        #     # '东单': 'dongdan',
        #     # '东花市': 'donghuashi',
        #     # '东四': 'dongsi1',
        #     # '东直门': 'dongzhimen',
        #     # '工体': 'gongti',
        #     # '广渠门': 'guangqumen',
        #     # '和平里': 'hepingli',
        #     # '建国门内': 'jianguomennei',
        #     # '建国门外': 'jianguomenwai',
        #     # '交道口': 'jiaodaokou',
        #     # '金宝街': 'jinbaojie',
        #     # '六铺炕': 'liupukang',
        #     # '蒲黄榆': 'puhuangyu',
        #     # '前门': 'qianmen',
        #     # '陶然亭': 'taoranting1',
        #     # '天坛': 'tiantan',
        #     # '西单': 'xidan',
        #     # '西罗园': 'xiluoyuan',
        #     # '洋桥': 'yangqiao1',
        #     # '永定门': 'yongdingmen',
        #     '左安门': 'zuoanmen1'
        # },
        # 'xicheng':{
        #     # '白纸坊': 'baizhifang1',
        #     # '菜户营': 'caihuying',
        #     # '长椿街': 'changchunjie',
        #     # '车公庄': 'chegongzhuang1',
        #     # '德胜门': 'deshengmen',
        #     # '地安门': 'dianmen',
        #     # '阜成门': 'fuchengmen',
        #     # '广安门': 'guanganmen',
        #     # '官园': 'guanyuan',
        #     # '金融街': 'jinrongjie',
        #     # '六铺炕': 'liupukang',
        #     # '马甸': 'madian1',
        #     # '马连道': 'maliandao1',
        #     # '木樨地': 'muxidi1',
        #     # '牛街': 'niujie',
        #     # '太平桥': 'taipingqiao1',
        #     # '陶然亭': 'taoranting1',
        #     # '天宁寺': 'tianningsi1',
        #     # '小西天': 'xiaoxitian1',
        #     # '西单': 'xidan',
        #     # '新街口': 'xinjiekou2',
        #     # '西四': 'xisi1',
        #     # '西直门': 'xizhimen1',
        #     # '宣武门': 'xuanwumen12',
        #     # '右安门内': 'youanmennei11',
        #     '月坛': 'yuetan'
        # },
        # 'fengtai':{
        #     # '北大地': 'beidadi',
        #     # '北京南站': 'beijingnanzhan1',
        #     # '菜户营': 'caihuying',
        #     # '草桥': 'caoqiao',
        #     # '成寿寺': 'chengshousi1',
        #     # '大红门': 'dahongmen',
        #     # '房山其它': 'fangshanqita',
        #     # '方庄': 'fangzhuang1',
        #     # '丰台其它': 'fengtaiqita1',
        #     # '广安门': 'guanganmen',
        #     # '和义': 'heyi',
        #     # '花乡': 'huaxiang',
        #     # '角门': 'jiaomen',
        #     # '旧宫': 'jiugong1',
        #     # '看丹桥': 'kandanqiao',
        #     # '科技园区': 'kejiyuanqu',
        #     # '刘家窑': 'liujiayao',
        #     # '六里桥': 'liuliqiao1',
        #     # '丽泽': 'lize',
        #     # '卢沟桥': 'lugouqiao1',
        #     # '马家堡': 'majiabao',
        #     # '马连道': 'maliandao1',
        #     # '木樨园': 'muxiyuan1',
        #     # '蒲黄榆': 'puhuangyu',
        #     # '七里庄': 'qilizhuang',
        #     # '青塔': 'qingta1',
        #     # '十里河': 'shilihe',
        #     # '宋家庄': 'songjiazhuang',
        #     # '太平桥': 'taipingqiao1',
        #     # '陶然亭': 'taoranting1',
        #     # '万源': 'wanyuan1',
        #     # '五棵松': 'wukesong1',
        #     # '五里店': 'wulidian',
        #     # '西红门': 'xihongmen',
        #     # '西罗园': 'xiluoyuan',
        #     # '新宫': 'xingong',
        #     # '洋桥': 'yangqiao1',
        #     # '永定门': 'yongdingmen',
        #     # '右安门外': 'youanmenwai',
        #     # '岳各庄': 'yuegezhuang',
        #     # '玉泉营': 'yuquanying',
        #     '赵公口': 'zhaogongkou'
        # },
        # 'shijingshan':{
        #     # '八角': 'bajiao1',
        #     # '城子': 'chengzi',
        #     # '古城': 'gucheng',
        #     # '老山': 'laoshan1',
        #     # '鲁谷': 'lugu1',
        #     # '苹果园': 'pingguoyuan1',
        #     # '石景山其它': 'shijingshanqita1',
        #     # '杨庄': 'yangzhuang1',
        #     '玉泉路': 'yuquanlu11'
        # },
        # 'daxing':{
        #     # '大兴机场空港': 'daxingjichangkonggang',
        #     # '大兴其它': 'daxingqita11',
        #     # '大兴新机场': 'daxingxinjichang',
        #     # '大兴新机场洋房别墅区': 'daxingxinjichangyangfangbieshuqu',
        #     # '高米店': 'gaomidian',
        #     # '观音寺': 'guanyinsi',
        #     # '和义': 'heyi',
        #     # '黄村火车站': 'huangcunhuochezhan',
        #     # '黄村中': 'huangcunzhong',
        #     # '旧宫': 'jiugong1',
        #     # '科技园区': 'kejiyuanqu',
        #     # '马驹桥': 'majuqiao1',
        #     # '南中轴机场商务区': 'nanzhongzhoujichangshangwuqu',
        #     # '天宫院': 'tiangongyuan',
        #     # '天宫院南': 'tiangongyuannan',
        #     # '通州其它': 'tongzhouqita11',
        #     # '万源': 'wanyuan1',
        #     # '西红门': 'xihongmen',
        #     # '新宫': 'xingong',
        #     # '义和庄': 'yihezhuang',
        #     # '亦庄': 'yizhuang1',
        #     # '亦庄开发区其它': 'yizhuangkaifaquqita1',
        #     # '枣园': 'zaoyuan',
        #     '瀛海': 'yinghai'
        # },
        # 'shunyi':{
        #     # '后沙峪': 'houshayu1',
        #     # '李桥': 'liqiao1',
        #     # '马坡': 'mapo',
        #     # '首都机场': 'shoudoujichang1',
        #     # '顺义城': 'shunyicheng',
        #     # '顺义其它': 'shunyiqita1',
        #     # '天竺': 'tianzhu1',
        #     '杨镇': 'yangzhen',
        #     '中央别墅区': 'zhongyangbieshuqu1'
        # },
        # 'changping':{
        #     # '奥林匹克公园': 'aolinpikegongyuan11',
        #     # '百善镇': 'baishanzhen',
        #     # '北七家': 'beiqijia',
        #     # '昌平其它': 'changpingqita1',
        #     # '东关': 'dongguan',
        #     # '鼓楼大街': 'guloudajie',
        #     # '海淀北部新区': 'haidianbeibuxinqu1',
        #     # '怀柔其它': 'huairouqita1',
        #     # '回龙观': 'huilongguan2',
        #     # '霍营': 'huoying',
        #     # '立水桥': 'lishuiqiao1',
        #     # '南口': 'nankou',
        #     # '南邵': 'nanshao',
        #     # '沙河': 'shahe2',
        #     # '天通苑': 'tiantongyuan1',
        #     # '小汤山': 'xiaotangshan1',
        #     # '西关环岛': 'xiguanhuandao',
        #     '西三旗': 'xisanqi1'
        # },
        # 'mentougou':{
        #     # '滨河西区': 'binhexiqu1',
        #     # '城子': 'chengzi',
        #     # '大峪': 'dayu',
        #     # '冯村': 'fengcun',
        #     # '门头沟其它': 'mentougouqita1',
        #     # '上岸地铁': 'shanganditie',
        #     '石门营': 'shimenying'
        # },
        # 'fangshan':{
        #     # '长阳': 'changyang1',
        #     # '城关': 'chengguan',
        #     # '大兴其它': 'daxingqita11',
        #     # '窦店': 'doudian',
        #     # '房山其它': 'fangshanqita',
        #     # '丰台其它': 'fengtaiqita1',
        #     # '韩村河': 'hancunhe1',
        #     # '良乡': 'liangxiang',
        #     # '琉璃河': 'liulihe',
        #     # '阎村': 'yancun',
        #     '燕山': 'yanshan'
        # },
        # 'huairou':{
        #     # '怀柔': 'huairouchengqu1',
        #     '怀柔其它': 'huairouqita1'
        # },
        # 'miyun':{
        #     # '北庄镇': 'beizhuangzhen',
        #     # '不老屯镇': 'bulaotunzhen',
        #     # '大城子镇': 'daichengzizhen',
        #     # '东邵渠镇': 'dongshaoquzhen',
        #     # '冯家峪镇': 'fengjiayuzhen',
        #     # '高岭镇': 'gaolingzhen',
        #     # '古北口镇': 'gubeikouzhen',
        #     # '鼓楼街道': 'guloujiedao',
        #     # '果园街道': 'guoyuanjiedao',
        #     # '河南寨镇': 'henanzhaizhen',
        #     # '巨各庄镇': 'jugezhuangzhen',
        #     # '密云其它': 'miyunqita11',
        #     # '密云镇': 'miyunzhen',
        #     # '穆家峪镇': 'mujiayuzhen',
        #     # '石城镇': 'shichengzhen',
        #     # '十里堡镇': 'shilipuzhen',
        #     # '太师屯镇': 'taishitunzhen',
        #     # '檀营': 'tanying',
        #     # '新城子镇': 'xinchengzizhen',
        #     # '西田各庄镇': 'xitiangezhuangzhen',
        #     '溪翁庄镇': 'xiwengzhuangzhen'
        # },
        'pinggu':{
            '平谷其它': 'pingguqita1'
        },
        'yanqing':{
            # '怀柔其它': 'huairouqita1',
            '延庆其它': 'yanqingqita1'
        },
        'tongzhou':{
            # '北关': 'beiguan',
            # '大兴新机场洋房别墅区': 'daxingxinjichangyangfangbieshuqu',
            # '果园': 'guoyuan1',
            # '九棵树(家乐福)': 'jiukeshu12',
            # '临河里': 'linheli',
            # '梨园': 'liyuan',
            # '潞苑': 'luyuan',
            # '马驹桥': 'majuqiao1',
            # '乔庄': 'qiaozhuang',
            # '首都机场': 'shoudoujichang1',
            # '通州北苑': 'tongzhoubeiyuan',
            # '通州其它': 'tongzhouqita11',
            # '万达': 'wanda14',
            # '武夷花园': 'wuyihuayuan',
            # '亦庄': 'yizhuang1',
            '玉桥': 'yuqiao'
        }

    }

    area_id_set= set()
    sub_city_set = set()
    house_data_list = []
    area_data_list = []
    run()
    print('线上数据处理完成，开始持久化数据')

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
