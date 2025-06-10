import json
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import get_response, save_result, save_result_txt


# 配置方法选择
search_model = 'gemini_search'  # 选择 'gemini_search' 或 'deer-flow'
question_model = 'gpt' #'claude-4-sonnet'

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

    import pdb; pdb.set_trace()

    
    # 生成问题
    for N_I_LOW, N_I_HIGH in [(3, 4), (5, 6)]:
        prompt = question_generate_prompt.replace('{entity}', entity).replace('{N_I_LOW}', str(N_I_LOW)).replace('{N_I_HIGH}', str(N_I_HIGH)).replace('{N_Q}', '3')
        messages.append({'role': 'user', 'content': prompt})
        response = get_response(model=question_model, messages=messages)

        if 'question_response' not in result:
            result['question_response'] = response
        else:
            result['question_response']['questions'] += response['questions']

    return result


parallel = False

def main():
    entities = ["阿蒙（诡秘之主）", "土伯（牧神记）", "丹妮莉丝·坦格利安（冰与火之歌）", "芙宁娜（原神）", "雪王（蜜雪冰城的吉祥物）", "进才中学（上海）", "Kano (鹿乃)", "Hamlet (character)", "Kyogre", "Sam Altman", "Ryner Lute", "Airi Tazume"]

    # 根据方法调整并发数
    
    results = {entity: None for entity in entities}

    if parallel:
        max_workers = 5 if search_model == 'gemini_search' else 1  # 
        print(f"最大并发数: {max_workers}")
        
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
    with open(f'results/{search_model}.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # 保存TXT格式的简化结果
    save_result_txt(f'results/{search_model}_simple.txt', results)

    print(f"📁 结果保存在 results/ 目录下（使用{search_model}方法）")
    print(f"📄 JSON格式: results/{search_model}.json")
    print(f"📄 TXT格式: results/{search_model}_simple.txt")

if __name__ == "__main__":
    main()