#!/usr/bin/env python3
"""
从browsecomp数据创建训练和测试数据集
- 读取browsecomp-zh.parquet和browsecomp.parquet文件
- 转换为指定格式，包含question, prompt, ground_truth和prompt_id字段
- 80%数据用于训练，20%用于测试
"""

import json
import random
import pandas as pd
from typing import List, Dict, Any
from collections import Counter

def load_parquet_data(file_path: str) -> pd.DataFrame:
    """加载parquet数据"""
    print(f"正在加载: {file_path}")
    df = pd.read_parquet(file_path)
    print(f"加载完成，包含 {len(df)} 条记录")
    return df

def convert_browsecomp_data(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """转换browsecomp数据格式"""
    questions_data = []
    
    for _, row in df.iterrows():
        # 提取字段
        question = row['prompt'][0]['content']
        prompt = row['prompt']
        ground_truth = row['reward_model']['ground_truth']
        prompt_id = row['extra_info']['index']
        data_source = row['data_source']
        
        questions_data.append({
            'question': question,
            'prompt': prompt,
            'ground_truth': ground_truth,
            'prompt_id': prompt_id,
            'data_source': data_source
        })
    
    print(f"转换了 {len(questions_data)} 条问题数据")
    return questions_data

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

    hdfs_file = 'hdfs://haruna/home/byte_data_seed/ssd_ygdt/user/zhanglin.0106/agent_rl/data/dr_train_for_seed1.6_filtered_only_open_17389.parquet'
    df = pd.read_parquet(hdfs_file)
    system_prompt = df['session'].iloc[0]['system_prompt']
    #return system_prompt
    system_prompt = json.loads(system_prompt)
    system_prompt['messages'][0]['content'] = """You are an online information search expert. Your task is to collect relevant information through online searches based on user questions and then answer those questions using the gathered information.\n\n# Task Description\nUpon receiving a user\'s question, you need to fully understand their needs, utilize the retrieval tools I provide, and analyze the corresponding information and data from multiple perspectives to answer the user\'s question.\n\nThe following principles must be adhered to during task execution:\n- **Fully Understand User Needs**: You must analyze the user\'s question from multiple angles, breaking it down if necessary, to ensure you grasp the primary intent of the question.\n- **Flexible Use of Tools**: Once you fully understand the user\'s needs, please use the tools I provide to retrieve information.\n    - If you reflect and find that the information previously obtained by the tool is incomplete or incorrect, making it insufficient to answer the user\'s question, please consider what additional information needs to be searched, whether the approach needs adjustment, and call the tool again to obtain complete information.\n    - You can also break down the information to be retrieved into multiple sub-questions, each specific enough to be retrieved independently.\n- **Diverse Answering Style**: You need to combine the acquired information to ensure your answers are concise and accurate.\n\nAdditionally, you possess the ability for deep thinking. Before replying to the user, you will engage in comprehensive deep thought. Your thought process should be enclosed within `<think>` and `</think>` tags, and based on this thought process, you will either call a tool to retrieve information or formulate the final answer.\nFor example: "<think>This is the thinking process...</think>This is the tool call" or "<think>This is the thinking process...</think>This is the final answer."\nFor all user requests, you must always think deeply before answering.\n\nWhen using tools, strictly follow this format:\n<|FunctionCallBegin|>[{"name": "function_name","parameters": {"param": "xxx"}}]<|FunctionCallEnd|>\n\n## Environment Information\nCurrent Location: Beijing\nCurrent Time: {2025-07-01 03:37:17 PM JST}"""
    
    system_prompt['tools'][0]['function']['description'] = "Bing Web Search API provides secure, ad-free, and location-aware search results that can extract relevant information from billions of web documents. With a single API call, you can leverage Bing's capabilities to search billions of web pages, images, videos, and news, helping your users find the content they need from the World Wide Web."
    system_prompt['tools'][0]['function']['parameters']['properties']["query"]['description'] = "Keywords for internet search. Use either the same language as the user's question, or English."
    system_prompt['tools'][0]['function']['parameters']['properties']['offset']['description'] = "Offset starting from 0, not exceeding 100"
    system_prompt['tools'][0]['function']['parameters']['properties']['count']['description'] = "Number of results per page, maximum 20, default 10"
    system_prompt['tools'][0]['function']['parameters']['properties']['mkt']['description'] = "Market codes, use en-US for English content search, use zh-CN for Chinese content search, default en-US"
    system_prompt['tools'][0]['function']['parameters']['properties']['mkt']['default'] = "en-US"

    system_prompt['tools'][1]['function']['description'] = "Use requests + BeautifulSoup to get the raw content of a webpage. If the raw content is too long, it will automatically trigger model summarization."
    system_prompt['tools'][1]['function']['parameters']['properties']["url"]['description'] = "The webpage URL to read"
    system_prompt['tools'][1]['function']['parameters']['properties']['title']['description'] = "Webpage title, optional"
    system_prompt['tools'][1]['function']['parameters']['properties']['length_limit']['description'] = "If the page content character count exceeds this value, the large model will summarize the webpage content, default 2000"
    system_prompt['tools'][1]['function']['parameters']['properties']['query']['description'] = "Used for targeted summarization by the large model, default value is 'None'."
    system_prompt['tools'][1]['function']['parameters']['properties']['query']['default'] = "None"

    system_prompt = json.dumps(system_prompt, ensure_ascii=False)

    return system_prompt

def convert_to_hdfs_format(data: pd.DataFrame, data_source: str) -> List[Dict[str, Any]]:
    """转换为HDFS格式"""
    sessions = []
    
    # 从HDFS文件中动态读取system_prompt
    system_prompt = get_system_prompt_from_hdfs()
    #import pdb; pdb.set_trace()
    for i, q in data.iterrows():
        session = {
            'ability': 'ToB/Agent',
            'abtag_response': '{"task": ["NLP任务-ToB-Agent"], "constraint": []}',
            'add_turn': None,
            'create_time': None,
            'data_source': data_source,
            'difficulty': None,
            'env_info': '',
            'ground_truth': json.dumps([q['reward_model']['ground_truth']], ensure_ascii=False),
            'instruct': '',
            'is_valid': True,
            'merge_source': data_source, #browsecomp or browsecomp-zh
            'message_id': None,
            'oj_feature': None,
            'old_ability': None,
            'origin_output': None,
            'prompt': q['prompt'], # ndarray
            'prompt_id': f'{data_source}_{i}',
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
            'task_name': 'deep_research_official_rl'
        }
        sessions.append({'session': session})
    
    return sessions

def main():
    # 设置随机种子
    random.seed(42)
    
    print("=" * 80)
    print("📊 创建BC问题训练数据集")
    print("=" * 80)
    
    for file in ['/root/wxt/DeepResearcher/data/browsecomp.parquet', '/root/wxt/DeepResearcher/data/browsecomp-zh.parquet']:
    # 1. 加载数据
        data = load_parquet_data(file)

        # 7. 转换为HDFS格式
        print("\n🔄 转换为HDFS格式")
        data_source = data_source=file.split('/')[-1].split('.')[0]
        sessions = convert_to_hdfs_format(data, data_source)
    
        # 8. 保存数据
        print("\n💾 保存数据")
        import pandas as pd
        
        output_df = pd.DataFrame(sessions)
    
        output_file = file.replace('.parquet', '-alphaseed.parquet')

        print(f"保存数据到: {output_file}")
    
        output_df.to_parquet(output_file, index=False)
    

if __name__ == "__main__":
    main()