from pymongo import MongoClient
from pathlib import Path
import copy
from tqdm import tqdm
import json
import re
from pymongo import UpdateOne
from pymongo import MongoClient, errors
import gridfs
from pathlib import Path
import copy
from tqdm import tqdm


class MongoDBAPI:
    def __init__(self, db_name: str = 'gender_traits_dataset', collection_name: str = 'fs'):
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def backup_collection(self, backup_file):
        with open(backup_file, 'w', encoding='utf-8') as f:
            documents = self.collection.find()

            for doc in documents:
                json_doc = json.dumps(doc, default=str, ensure_ascii=False)
                f.write(json_doc + '\n')
        print(f"集合备份完成，文件保存在: {backup_file}")

    def restore_collection(self, backup_file):
        with open(backup_file, 'r', encoding='utf-8') as f:
            for line in f:
                doc = json.loads(line)
                self.collection.insert_one(doc)
        print(f"集合恢复完成，数据来自: {backup_file}")

    def insert_record(self, record):
        # todo 首先检查是否已经存在
        return self.collection.insert_one(record)


if __name__ == '__main__':
    mongo = MongoDBAPI()
    client = mongo.client
    db = client['pixiv']
    collection = db['tsetCollection']
    document = {'id': 123265856, 'title': '普普通通的每日修行', 'type': 'illust',
                'image_urls': {'square_medium': 'https://i.pximg.net/c/360x360_10_webp/img-master/img/2024/10/12/20/30/56/123265856_p0_square1200.jpg',
                               'medium': 'https://i.pximg.net/c/540x540_70/img-master/img/2024/10/12/20/30/56/123265856_p0_master1200.jpg',
                               'large': 'https://i.pximg.net/c/600x1200_90_webp/img-master/img/2024/10/12/20/30/56/123265856_p0_master1200.jpg'},
                'caption': '“那就预祝旅行者新婚快乐，哈哈。一转眼璃月的英雄也是落了根了，新娘还是那位仙女，真是英雄配美人！般配！哈哈”<br />“哪里的话博来老板，时势造英雄罢，我与阿鹤能相爱走到现在，离不开璃月港的各位的照顾！还请到时...”<br />“你这金毛大眼的，倒是把璃月这套学得有模有样哈哈，你是我们公认的英雄，不妨再多骄傲一点。那位璃月仙女还得你多照顾，到时传到国外，别说我们亏待英雄，声名远扬的旅行者娶了个璃月花瓶，虽颇有姿色，但平时又呆又....”<br />“诶！苏二娘！乱说什么呢！”<br />  博来用胳膊肘碰了一旁的苏二娘<br />“啊！唉，你看我这人，嘴巴碎，旅行者莫见怪..”<br />“没事..我知道苏二娘的意思。博来老板，我就送到这，再往前，你们自然就走出这洞天了”<br />  <br />那时，是成亲前的尘歌壶聚餐。璃月港的熟人们都来参加了。但心里清楚，比起阿鹤，富人家的千金或公主才更符合大家对英雄贤内的想象<br />即使阿鹤的外貌身材，都美到足以让任何人目瞪口呆。但是山里来的野丫头，还是成了固有印象。没礼数，与人疏离，没常识，不知轻重<br />申鹤没有出席，也没有去找，宾客们识趣地不问不提，完成了聚餐<br />不在阁楼，也不在二人同住的房间。便想到这洞天里唯一能看月的小屋，本打算修作库房，但是没能完工<br /><br />敲了敲木门<br />听到长发擦过衣服的声音，笑了笑<br />“阿鹤？”<br /><br />推开门，纤腰玉肢的仙女斜靠在木墙边，月色洒落在大长辫上，缓缓滑下，闪烁着柔和的银光<br />摩挲着肩上的红绳，这是阿鹤焦躁时的习惯<br />注意到这，轻脚走到侧身<br />“在看月亮吗？”<br />曾冷若冰霜的眼眸低垂着，小巧红艳的嘴唇微微颤动<br />手上传来熟悉的感觉，是阿鹤如凝脂般细滑的纤纤玉手<br />“旅行者..”<br />“阿鹤在想什么，我都知道。我可是阿鹤的红绳”<br />缓缓拉过申鹤的手<br />“不论阿鹤怎么想，我都永远不会质疑你。我们彼此相望着走到了一起，是我们选择了我们。”<br />阿鹤高挑的身段，银白长发以及冷艳美丽的仙女外貌，让人们容易误解她。仅从外在看，阿鹤毫无疑问是一位顶级御姐。但每日相处，才明白。内里，只是一位不知所措，对喜欢的人言听计从的少女。<br />感受到手里的力度渐渐增加，仙女依偎进怀里<br />轻抚阿鹤的脑袋<br /><br />“喜欢旅行者，喜欢夫君。”<br />“诶？阿鹤从哪学的，现在就叫夫君了吗...有点害羞了...”<br />“师傅藏书阁里看到的....已婚女性都这么称呼丈夫....”<br />“我也喜欢阿鹤。”<br /> 怀里的脑袋蹭蹭，贴得更紧，阿鹤嘴里发出的声音都闷闷不清了<br /><br />“嗯..”<br /><br />“嗯 啊..啊啊~!！哦噢..啊啊..！哦啊啊.！..”<br /><br />撞肉声从那个小破屋里传出，和当时一样没有完工。但房间里充满了使用的痕迹。<br />阿鹤曾经斜靠的木墙上，钉满了钉子，钉下挂着充满了元素能的符。这种钉子是由铁和元素矿石制成。可以引导出符咒中的元素力，通过与红绳连接，让能量流动在被红绳束缚的道具上<br />为何选择红绳，重云的解释是，阿鹤平时穿戴的这种红绳，对阳气很敏感，是完美介质。二是阿鹤常穿戴，家中备绳多，若想修行，随时将阿鹤束缚起来就好<br /><br />“呼~..出精量..稳住..好..耐心..记住感觉..”（啪啪啪）<br /><br />敲了敲木门，推开<br />“重、重云？修行还没结束？差不多该..吃晚饭..”<br />“姨父！咱还不饿，这次修行想试看能否突破瓶颈”<br />“那个..你定是不饿..放你小姨下来休息..吃点..”<br /> “哈哈姨父不是早就知道，小姨作为我鸡巴套子的功能吗，子宫吸收我的纯阳液就能维持体力，不需休息和食物。委婉劝我休息，不愧是姨父，多谢关心。”<br />“....”<br />重云保持着插入节奏，每当龟头贯穿阿鹤的子宫口后，稍微停留，再拔出龟头至不会离开小穴的最大限。汇聚阳气，将力集中在腰，发力，再次贯穿阿鹤的子宫<br />每一次撞击，阿鹤的背便被推荡到木墙，挤压拘束在背后的双手，木板嘎吱作响<br />平时夫妻同床时，阿鹤会小心害羞地把娇嫩的美脚放在旅行者小腿之间，轻蹭着皮肤，表达对夫君的爱意。现在，这双黑丝玉足，悬挂红绳之下，随着重云每一次撞击在空中无力地晃动着<br />“那个..重云..现在这，是个什么修行呐....”<br />“姨父感到好奇吗？是修炼我纯阳之体细微控制的修行。每一次将等量精液注入申鹤小姨子宫。像挥剑，尽力保持每一次挥剑的力道相同！姨父能注意到我每次贯穿申鹤小姨子宫口，插到最深处时，会停下一拍吧？其实是在注精，快速射出等量的精液。控制，是这次的难点！”<br />“..原、原来如此..专注度..很高呢..但这么久修行..纯阳之体有暴走风险吗..平时都是大量射..现在要控制出精量..”<br /> “风险定是有，但为修行，总有办法！姨父一说，我就有些燥热了..没事，有这些符咒帮助，纯阳之力能最大限度地在作为我鸡巴套子的申鹤小姨体内流动，从已经射入子宫内的精液出发，与符中的能量共同汇入小姨的大脑，再借由这枚符..”<br />说罢，重云一边继续插入着，一边掏出一张符，贴在阿鹤的脑门上<br />“嗯啊啊！！..！..噢啊啊哦...哦啊啊..!..! <br />阿鹤全身痉挛着，元素力汇向符咒，过量元素让阿鹤的双眼泛着蓝光，红舌向外吐出<br />“阿、阿鹤？！”<br />腹部猛烈的紧缩着，似乎是要将重云填满的精液挤压出去，但奈何重云巨大的阳具贯穿子宫口，严丝合缝地堵塞着，哪怕一滴重云的精液也没能从阿鹤子宫里流出<br />并非阿鹤的子宫抵触重云的精液。相反，为了能容纳更多精液，肉体才本能想要将旧液排出，以迎接主人最新鲜滚烫的精液<br />重云的纯阳之体让他极易暴走，欲火缠身，体温远高常人，阳气极重，妖邪都无法近他分毫。这样的躯体当然不是偶然而生，在璃月阴阳至理下，阴阳相生相吸，自然会诞生与纯阳之体匹配，为吸收阳气，替阳体降温的纯阴之体。<br />食山间露水的申鹤，不论是修炼与重云同源的驱邪术法，还是拥有相同的世家血脉。甚至命格，便是孤辰劫煞的绝对阴气之躯。再配上冰系神之眼，这让她成为完美适配重云的冰凉鸡巴套子。成为替重云降温去火，抑制暴走而生的泄火飞机杯。连阴道内每一处的起伏，都完美契合重云阳具的形状<br />“呼..瞬间变凉快了，修行能继续了，还好姨父提醒，不然就有暴走风险了”<br />“不用谢.？”<br /> 重云深吸吐气，抓住阿鹤脖上的狗链，踮脚，吮吸着红唇，舌头在阿鹤口中搅动，灌入唾液。昏迷的阿鹤本能地大口吞咽着重云的口水，任由粘稠的唾液进入喉穴。下半身也未停止，抽插保持着之前节奏。蓄力，贯穿，注精，拔至穴口；发力，再次贯穿，注精，拔至穴口；发力，继续贯穿...<br />刚射入的精液无法排出，新的精液又继续灌入，阿鹤的肚子渐渐膨胀<br />阴阳是自然法则一环，阿鹤确是自己妻子，但那之前，她更是重云的专用飞机杯，降温鸡巴套子。降临者的身份，不属于提瓦特。若干涉提瓦特阴阳至理，也许会伤害到阿鹤<br />而阿鹤自己又无法反抗，纯阳之体与极阴之躯之间无限高的相性在肉体层面否定了反抗的可能性<br />“阿鹤..”<br />似乎改变了节奏？重云朝着阿鹤的右脸扇了一记耳光，然后便是三下猛烈抽插。反向挥，继续扇在阿鹤的左脸。接着又是三下大力抽插，<br />“这是！？”<br />“姨父别急，用元素视野看。这是小姨的用法。”<br />立刻用元素视野观察，每当重云扇阿鹤一巴掌时，阿鹤的子宫便会抽动，加速精液吸收，仿佛重云的耳光是对子宫的督促。三下猛烈的抽插，也压缩子宫里的精液，变相帮助吸收<br />如此，对重云的做法也无话可说了。毕竟，重云只是在利用技巧使用自己的飞机杯而已<br />啪-啪啪啪！啪-啪啪啪！啪-啪啪啪！啪-啪啪啪！<br />一阵眩晕<br />“.姨父不舒服..先休息了..饭菜在桌上...“',
                'restrict': 0, 'user': {'id': 67227995, 'name': 'HVVT', 'account': '935963', 'profile_image_urls': {
            'medium': 'https://i.pximg.net/user-profile/img/2022/07/19/15/14/09/23048070_41eec0eff1b8ab6b92afe9b5e2d5ab83_170.jpg'}, 'is_followed': False},
                'tags': [{'name': 'R-18', 'translated_name': None}, {'name': '原神', 'translated_name': 'Genshin Impact'}, {'name': '申鶴', 'translated_name': 'Shenhe'},
                         {'name': '申鹤', 'translated_name': 'Shenhe'}, {'name': '重雲', 'translated_name': 'Chongyun'}, {'name': 'NTR', 'translated_name': 'cuckold'},
                         {'name': 'アヘ顔', 'translated_name': 'ahegao'}, {'name': 'ボテ腹', 'translated_name': 'bloated belly'}, {'name': '拘束', 'translated_name': 'bondage'},
                         {'name': 'レイプ', 'translated_name': 'rape'}], 'tools': [], 'create_date': '2024-10-12T20:30:56+09:00', 'page_count': 4, 'width': 3615, 'height': 3828,
                'sanity_level': 6, 'x_restrict': 1, 'series': None, 'meta_single_page': {}, 'meta_pages': [{'image_urls': {
            'square_medium': 'https://i.pximg.net/c/360x360_10_webp/img-master/img/2024/10/12/20/30/56/123265856_p0_square1200.jpg',
            'medium': 'https://i.pximg.net/c/540x540_70/img-master/img/2024/10/12/20/30/56/123265856_p0_master1200.jpg',
            'large': 'https://i.pximg.net/c/600x1200_90_webp/img-master/img/2024/10/12/20/30/56/123265856_p0_master1200.jpg',
            'original': 'https://i.pximg.net/img-original/img/2024/10/12/20/30/56/123265856_p0.jpg'}}, {'image_urls': {
            'square_medium': 'https://i.pximg.net/c/360x360_10_webp/img-master/img/2024/10/12/20/30/56/123265856_p1_square1200.jpg',
            'medium': 'https://i.pximg.net/c/540x540_70/img-master/img/2024/10/12/20/30/56/123265856_p1_master1200.jpg',
            'large': 'https://i.pximg.net/c/600x1200_90_webp/img-master/img/2024/10/12/20/30/56/123265856_p1_master1200.jpg',
            'original': 'https://i.pximg.net/img-original/img/2024/10/12/20/30/56/123265856_p1.jpg'}}, {'image_urls': {
            'square_medium': 'https://i.pximg.net/c/360x360_10_webp/img-master/img/2024/10/12/20/30/56/123265856_p2_square1200.jpg',
            'medium': 'https://i.pximg.net/c/540x540_70/img-master/img/2024/10/12/20/30/56/123265856_p2_master1200.jpg',
            'large': 'https://i.pximg.net/c/600x1200_90_webp/img-master/img/2024/10/12/20/30/56/123265856_p2_master1200.jpg',
            'original': 'https://i.pximg.net/img-original/img/2024/10/12/20/30/56/123265856_p2.jpg'}}, {'image_urls': {
            'square_medium': 'https://i.pximg.net/c/360x360_10_webp/img-master/img/2024/10/12/20/30/56/123265856_p3_square1200.jpg',
            'medium': 'https://i.pximg.net/c/540x540_70/img-master/img/2024/10/12/20/30/56/123265856_p3_master1200.jpg',
            'large': 'https://i.pximg.net/c/600x1200_90_webp/img-master/img/2024/10/12/20/30/56/123265856_p3_master1200.jpg',
            'original': 'https://i.pximg.net/img-original/img/2024/10/12/20/30/56/123265856_p3.jpg'}}], 'total_view': 38196, 'total_bookmarks': 6437, 'is_bookmarked': False,
                'visible': True, 'is_muted': False, 'total_comments': 65, 'illust_ai_type': 1, 'illust_book_style': 0, 'comment_access_control': 0}
    result = collection.insert_one(document)
    print(f"Inserted document id: {result.inserted_id}")

    # mongo.backup_collection(backup_file=r'E:\MongoDB\data\bakckup04.json')
