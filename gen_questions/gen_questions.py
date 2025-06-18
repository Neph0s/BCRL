import json
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import get_response, save_result, save_result_txt, extract_json, ensure_question_format


# 配置方法选择
search_model = 'gemini_search'  # 选择 'gemini_search' 或 'deer-flow'
question_model = 'claude-4-sonnet' #'gpt'

def process_entity(entity):
    """处理单个实体的函数，用于并发执行"""
    print(f"开始查询实体: {entity} (使用方法: {search_model})")
    
    result = {'entity': entity} 
    
    from prompts import search_prompt, search_second_prompt,  question_generate_prompt

    messages = []

    # 搜集实体信息
    prompt = search_prompt.replace('{entity}', entity)
    messages.append({'role': 'user', 'content': prompt})
    knowledge = get_response(model=search_model, messages=messages)
    result['search_response'] = knowledge
    messages.append({'role': 'assistant', 'content': knowledge})

    # 二次扩展
    messages.append({'role': 'user', 'content': search_second_prompt})
    knowledge2 = get_response(model=search_model, messages=messages)
    result['search_again_response'] = knowledge2
    messages.append({'role': 'assistant', 'content': knowledge2})
    
    # 生成问题
    for N_I_LOW, N_I_HIGH in [(3, 4), (5, 6)]:
        prompt = question_generate_prompt.replace('{entity}', entity).replace('{N_I_LOW}', str(N_I_LOW)).replace('{N_I_HIGH}', str(N_I_HIGH)).replace('{N_Q}', '3')

        messages.append({'role': 'user', 'content': prompt})
        response = get_response([extract_json, ensure_question_format], model=question_model, messages=messages)

        if 'question_response' not in result:
            result['question_response'] = []
        
        result['question_response'].append(response)
        
        messages.pop()

        # 离线判定答案唯一性 TODO

    return result


parallel = False

def main():
    # 我熟悉的10个实体
    entities = ["阿蒙（诡秘之主）", "土伯（牧神记）", "丹妮莉丝·坦格利安（冰与火之歌）", "芙宁娜（原神）", "雪王（蜜雪冰城的吉祥物）", "上海市进才中学", "鹿乃（唱见）", "哈姆雷特", "盖欧卡", "Sam Altman"]

    # 从“中文实体”中随机采样的90个实体
    entities += ['国际海事组织', '免疫疗法', '圣雅各大教堂', '圣马可大教堂', '莫斯科战役', '第聂伯河', '承德避暑山庄', '中国科学院大气物理研究所', '玛雅文明', '保加利亚与罗马尼亚联盟', '圣多马大教堂', '莫奈', '新加坡国立大学医学院', '中国科学院计算技术研究所', '东正教', '流行性感冒', '生物多样性公约', '携手霍桑', '北京大学', '巴黎气候协定', '普朗克', '金砖国家', '邓小平', '中国科学院广州能源研究所', '浙江大学', '巴黎圣母院', '婆罗浮屠', '克隆技术', '田纳西河', '长江三峡', '李大钊', '法国世界杯', '农发行', '多瑙河', '林肯', '巴黎协定', '联合国人口基金', '和平银元', '怀俄明河', '中国地质大学', '罗斯福十美分硬币', '阿拉巴马州百年纪念半美元', '艾夫斯', '瓜达尔基维尔河', '叙利亚内战', '毛泽东选集', '大分水岭', '圣安德烈大教堂', '古安息语', '波斯波利斯', '甲骨文', 'CNN', '生物多样性公约', '北京大学', '圣西门大教堂', '国际足联', '北大西洋公约组织', '中国政法大学', '北斗导航', '古满语', '巴颜喀拉山脉', '冲绳战役', '人工神经递质', '苏轼', '圣达太大教堂', '国际跳水联合会', '所罗门王', '马来王猪笼草', '复旦大学', '金朝', '天津水上公园', '中国科学院深圳先进技术研究院', '国际国际象棋联合会', '中国科学院生物物理研究所', '人工生命', '有声内容图标', '世界卫生组织', '火星探测车', '青铜器', '苏-27战斗机', '哥伦布半美元', '萨拉热窝', '中国科学院生物物理研究所', '煎饼馃子', '西夏王陵', '卢浮宫', '嵩山', '自由女神像', '中国高铁', '京津冀协同发展']

    entities = entities[:10]#50]

    # 根据方法调整并发数
    
    results = {entity: None for entity in entities}

    if parallel:
        max_workers = 5 
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_entity = {executor.submit(process_entity, entity): entity for entity in entities}
            
            # 处理完成的任务
            for future in as_completed(future_to_entity):
                entity = future_to_entity[future]
                result = future.result()
                results[entity] = result

    else:
        for entity in entities:
            result = process_entity(entity)
            results[entity] = result

    completed = len([result for result in results.values() if result is not None])
    failed = len(entities) - completed
    
    print(f"✅ 成功: {completed}, ❌ 失败: {failed}")

    # 保存汇总结果
    os.makedirs("results", exist_ok=True)
    with open(f'results/{question_model}.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # 保存TXT格式的简化结果
    save_result_txt(f'results/{question_model}_simple.txt', results)

    print(f"📁 结果保存在 results/ 目录下（使用{question_model}方法）")
    print(f"📄 JSON格式: results/{question_model}.json")
    print(f"📄 TXT格式: results/{question_model}_simple.txt")

def print_questions():
    with open('results/claude-4-sonnet.json', 'r', encoding='utf-8') as f:
        results = json.load(f)

    simple_results = {entity: results[entity]['question_response'] for entity in results}
    # for entity, result in results.items():
    #     print(entity)
    #     for question in result['question_response']:
    #         print(json.dumps(question, indent=4, ensure_ascii=False))

    with open('results/bc_questions_0612.json', 'w', encoding='utf-8') as f:
        json.dump(simple_results, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    main()
    #print_questions()