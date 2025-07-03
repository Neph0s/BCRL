#!/usr/bin/env python3
"""
从bc_questions数据创建训练和测试数据集
- 中文数据取前5k个实体，英文数据取前10k个实体
- 每个实体取2-3个问题，用第一个实体类型填入{entity_type}
- 80%实体用于训练，20%用于测试
- 训练集取3w个问题，测试集取3k个问题（每个实体只取1个）
"""

import json
import random
from typing import List, Dict, Any
from collections import Counter

def load_json_data(file_path: str) -> Dict[str, Any]:
    """加载JSON数据"""
    print(f"正在加载: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"加载完成，包含 {len(data)} 个实体")
    return data

def extract_entities_and_questions(data: Dict[str, Any], max_entities: int) -> List[Dict[str, Any]]:
    """提取实体和问题数据"""
    entities_data = []
    
    count = 0
    for entity_key, entity_data in data.items():
        if count >= max_entities:
            break
            
        entity_name = entity_data.get('entity', '')
        entity_info = entity_data.get('entity_info', {})
        
        wiki_id = entity_info.get('id')
        
        # 获取问题数据
        question_response = entity_data.get('question_response')
        if not question_response:
            continue
        
        try:
            entity_type = entity_data['question_response'][0]['questions'][0]['entity_type'][0]
        except:
            entity_type = None

        # 提取问题列表
        questions = []
        for qr in question_response:
            if qr and 'questions' in qr:
                for q in qr['questions']:
                    if isinstance(q, dict) and 'question' in q:
                        questions.append(q['question'].replace('{entity_type}', q['entity_type'][0]))
        
        if len(questions) < 2:
            continue
        

        entities_data.append({
            'entity': entity_name,
            'entity_type': entity_type,
            'wiki_id': wiki_id,
            'questions': questions
        })
        
        count += 1
    
    print(f"提取了 {len(entities_data)} 个有效实体")
    return entities_data

def split_train_test(entities_data: List[Dict[str, Any]], train_ratio: float = 0.8) -> tuple:
    """按80:20比例分割训练和测试实体"""
    random.shuffle(entities_data)
    
    train_size = int(len(entities_data) * train_ratio)
    train_entities = entities_data[:train_size]
    test_entities = entities_data[train_size:]
    
    print(f"训练实体: {len(train_entities)} 个")
    print(f"测试实体: {len(test_entities)} 个")
    
    return train_entities, test_entities

def sample_questions(train_entities: List[Dict[str, Any]], query_per_entity: int = 2) -> List[Dict[str, Any]]:
    """从训练实体中采样问题，目标30k个"""
    all_questions = []
    entity_counter = {}  # 统计每个实体类型的数量
    
    for entity_data in train_entities:
        entity = entity_data['entity']
        entity_type = entity_data['entity_type']
        wiki_id = entity_data['wiki_id']
        questions = entity_data['questions']
        
        # 统计实体类型
        if entity_type not in entity_counter:
            entity_counter[entity_type] = 0
        entity_counter[entity_type] += 1

        entity_type_idx = 0
        
        for question_idx, question in enumerate(questions[:query_per_entity]):
            # 构造prompt_id: 实体名+wikiqid+第几个问题+第几个实体类型
            prompt_id = f"bcsyn_{entity}_{wiki_id}_q{question_idx}_t{entity_type_idx}"
            
            all_questions.append({
                'entity': entity,
                'entity_type': entity_type,
                'wiki_id': wiki_id,
                'question': question,
                'prompt': [{'content': question, 'role': 'user'}],
                'ground_truth': [entity],  # 简化的ground_truth
                'data_source': 'bc_questions',
                'ability': 'ToB/Agent',
                'task_name': 'deep_research_official_rl',
                'prompt_id': prompt_id,
                # 'entity_idx': entity_idx,
                # 'question_idx': question_idx
            })
    
    
    print(f"训练问题: {len(all_questions)} 个")
    return all_questions

def get_system_prompt_from_hdfs():
    """从HDFS文件中读取system_prompt"""
    import pandas as pd
    hdfs_file = 'hdfs://haruna/home/byte_data_seed/ssd_ygdt/user/zhanglin.0106/agent_rl/data/dr_train_for_seed1.6_filtered_only_open_17389.parquet'
    df = pd.read_parquet(hdfs_file)
    system_prompt = df['session'].iloc[0]['system_prompt']
    #return system_prompt
    system_prompt = json.loads(system_prompt)
    system_prompt['messages'][0]['content'] = """You are an online information search expert. Your task is to collect relevant information through online searches based on user questions and then answer those questions using the gathered information.\n\n# Task Description\nUpon receiving a user\'s question, you need to fully understand their needs, utilize the retrieval tools I provide, and analyze the corresponding information and data from multiple perspectives to answer the user\'s question.\n\nThe following principles must be adhered to during task execution:\n- **Fully Understand User Needs**: You must analyze the user\'s question from multiple angles, breaking it down if necessary, to ensure you grasp the primary intent of the question.\n- **Flexible Use of Tools**: Once you fully understand the user\'s needs, please use the tools I provide to retrieve information.\n    - If you reflect and find that the information previously obtained by the tool is incomplete or incorrect, making it insufficient to answer the user\'s question, please consider what additional information needs to be searched, whether the approach needs adjustment, and call the tool again to obtain complete information.\n    - You can also break down the information to be retrieved into multiple sub-questions, each specific enough to be retrieved independently.\n- **Diverse Answering Style**: You need to combine the acquired information to ensure your answers are concise and accurate.\n\nAdditionally, you possess the ability for deep thinking. Before replying to the user, you will engage in comprehensive deep thought. Your thought process should be enclosed within `<think>` and `</think>` tags, and based on this thought process, you will either call a tool to retrieve information or formulate the final answer.\nFor example: "<think>This is the thinking process...</think>This is the tool call" or "<think>This is the thinking process...</think>This is the final answer."\nFor all user requests, you must always think deeply before answering.\n\nWhen using tools, strictly follow this format:\n<|FunctionCallBegin|>[{"name": "function_name","parameters": {"param": "xxx"}}]<|FunctionCallEnd|>\n\n## Environment Information\nCurrent Location: Beijing\nCurrent Time: {2025-07-01 03:37:17 PM JST}"""

    system_prompt = json.dumps(system_prompt, ensure_ascii=False)

    return system_prompt
def convert_to_hdfs_format(questions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """转换为HDFS格式"""
    sessions = []
    
    # 从HDFS文件中动态读取system_prompt
    system_prompt = get_system_prompt_from_hdfs()
    
    for q in questions:
        session = {
            'ability': q['ability'],
            'abtag_response': '{"task": ["NLP任务-ToB-Agent"], "constraint": []}',
            'add_turn': None,
            'create_time': None,
            'data_source': q['data_source'],
            'difficulty': None,
            'env_info': '',
            'ground_truth': json.dumps(q['ground_truth'], ensure_ascii=False),
            'instruct': '',
            'is_valid': True,
            'merge_source': 'wxt_bc',
            'message_id': None,
            'oj_feature': None,
            'old_ability': None,
            'origin_output': None,
            'prompt': q['prompt'],
            'prompt_id': q['prompt_id'],
            'remark': None,
            'response_input': None,
            'reward_strategy': json.dumps([{"name": "opbm_search", "type": "GenerativeRewardModel", "weight": 1.0, "weight_type": "soft", "need_thinking": False, "strategy_extra_infos": {}}], ensure_ascii=False),
            'rid': None,
            'sd_response': None,
            'session_id': None,
            'source': 'tob',
            'source_type': None,
            'subject': None,
            'system_prompt': system_prompt,
            'task_name': q['task_name']
        }
        sessions.append({'session': session})
    
    return sessions

def main():
    # 设置随机种子
    random.seed(42)
    
    print("=" * 80)
    print("📊 创建BC问题训练数据集")
    print("=" * 80)
    
    # 1. 加载数据
    zh_data = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0628_zh_v2json')
    en_data = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0629_en_v2.json')
    
    # 2. 提取实体和问题
    print("\n🔍 提取中文实体和问题 (前5k个实体)")
    zh_entities = extract_entities_and_questions(zh_data, 5000)
    
    print("\n🔍 提取英文实体和问题 (前10k个实体)")
    en_entities = extract_entities_and_questions(en_data, 10000)
    
    # 3. 合并中英文数据
    all_entities = zh_entities + en_entities
    print(f"\n📊 总实体数: {len(all_entities)}")
    
    # 4. 分割训练和测试
    print("\n✂️ 分割训练和测试数据 (80:20)")
    train_entities, test_entities = split_train_test(all_entities)
    
    # 5. 采样训练问题
    print("\n🎯 采样训练问题 (目标30k)")
    train_questions = sample_questions(train_entities, 2)
    
    # 6. 采样测试问题
    print("\n🎯 采样测试问题 (目标3k，每个实体1个)")
    test_questions = sample_questions(test_entities, 1)
    
    # 7. 转换为HDFS格式
    print("\n🔄 转换为HDFS格式")
    train_sessions = convert_to_hdfs_format(train_questions)
    test_sessions = convert_to_hdfs_format(test_questions)
    
    # 8. 保存数据
    print("\n💾 保存数据")
    import pandas as pd
    
    train_df = pd.DataFrame(train_sessions)
    test_df = pd.DataFrame(test_sessions)
    
    train_file = '/root/wxt/bc/gen_questions/results/bc_train_data_30k_alphaseed.parquet'
    test_file = '/root/wxt/bc/gen_questions/results/bc_test_data_3k_alphaseed.parquet'
    
    train_df.to_parquet(train_file, index=False)
    test_df.to_parquet(test_file, index=False)
    
    print(f"训练数据已保存: {train_file} ({len(train_df)} 条记录)")
    print(f"测试数据已保存: {test_file} ({len(test_df)} 条记录)")
    
    # 9. 统计信息
    print("\n📈 数据统计")
    print(f"训练数据: {len(train_df)} 条")
    print(f"测试数据: {len(test_df)} 条")
    
    # 实体类型统计
    train_types = [q['entity_type'] for q in train_questions]
    test_types = [q['entity_type'] for q in test_questions]
    
    print("\n🏷️ 训练数据实体类型分布:")
    for entity_type, count in Counter(train_types).most_common(10):
        print(f"  {entity_type}: {count}")
    
    print("\n🏷️ 测试数据实体类型分布:")
    for entity_type, count in Counter(test_types).most_common(10):
        print(f"  {entity_type}: {count}")
    
    print("\n✅ 数据处理完成！")
    print("=" * 80)

if __name__ == "__main__":
    main()