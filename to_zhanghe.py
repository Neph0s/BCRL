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
import pandas as pd 

source = ['bc-syn', 'deep_researcher', 'web_walker', 'to_zhanghe'][0]

def load_json_data(file_path: str) -> Dict[str, Any]:
    """加载JSON数据"""
    print(f"正在加载: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"加载完成，包含 {len(data)} 个实体")
    return data

def extract_entities_and_questions(data: Dict[str, Any], max_entities: int, expand_mode=False) -> List[Dict[str, Any]]:
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
                        if len(q['entity_type']) == 1: continue
                        questions.append(q)
                        #questions.append(q['question'].replace('{entity_type}', q['entity_type'][0]))
        
        if len(questions) < 6 and not expand_mode:
            continue
        

        entities_data.append({
            'entity': entity_name,
            'entity_type': entity_type,
            'wiki_id': wiki_id,
            'popularity_score': entity_info['popularity_score'],
            'questions': questions
        })
        
        count += 1
    
    print(f"提取了 {len(entities_data)} 个有效实体")
    return entities_data

def split_train_test(entities_data: List[Dict[str, Any]], train_ratio: float = 0.9) -> tuple:
    """按80:20比例分割训练和测试实体"""
    random.shuffle(entities_data)
    
    train_size = int(len(entities_data) * train_ratio)
    train_entities = entities_data[:train_size]
    test_entities = entities_data[train_size:]
    
    print(f"训练实体: {len(train_entities)} 个")
    print(f"测试实体: {len(test_entities)} 个")
    
    return train_entities, test_entities

def sample_questions(train_entities: List[Dict[str, Any]], training = True, expand_mode=False, rl_mode=False, n_questions=None) -> List[Dict[str, Any]]:
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
        
        if training:
            if source == 'to_zhanghe':
                idx_pairs = []
                if expand_mode:
                    for i in range(len(questions)):
                        if i % 3 == 0:
                            idx_pairs.append((i, 0))
                        elif i % 3 == 1:
                            idx_pairs.append((i, -1))
                        else:
                            pass
                    # for j in range(len(questions[i]['entity_type'])):
                    #     idx_pairs.append((i, j))
                    # 这样太多了。。40w
                else:
                    idx_pairs = [(0, 0), (1, -1), (3, 0), (5, -1)]
                    
            else:
                if rl_mode:
                    idx_pairs = [(2, 0), (4, -1)]
                else:
                    idx_pairs = [(0, 0), (1, -1), (3, 0), (5, -1)]
            # 第一个数字：第几道题
            # 第二个数字：0表示最宽泛的类型，-1表示最具体的类型
        else:
            idx_pairs = [(0, 0)]
        
        pivot_entity = entity
        for question_idx, entity_type_idx in idx_pairs:
            try:
                question = questions[question_idx]
            except:
                continue 

            if expand_mode:
                try:
                    entity = question['entity']
                except:
                    # bug question, skip 
                    continue 

            if entity_type_idx == -1:
                entity_type_idx = len(question['entity_type']) - 1
                assert(entity_type_idx > 0)

            # 构造prompt_id: 实体名+wikiqid+第几个问题+第几个实体类型
            prompt_id = f"bcsyn_{pivot_entity}_{wiki_id}_q{question_idx}_t{entity_type_idx}"
            if expand_mode:
                prompt_id += '_expand'
            
            entity_type_ = question['entity_type'][entity_type_idx]
            query = question['question'].replace('{entity_type}', entity_type_)
            import math
            try:
                easiness = math.log(entity_data['popularity_score'], 2)
            except:
                easiness = 1
            import random
            # 均匀分布，范围为[-1, 1] 
            easiness += random.uniform(-10, 10)

            if entity_type_idx != 0: easiness += 5
            if question_idx >= 3: easiness += 2
            
            sample = {
                'entity': entity,
                'entity_type': entity_type_,
                'priority': easiness,
                'wiki_id': wiki_id,
                'question': query,
                'prompt': query,#[{'content': query, 'role': 'user'}],
                'ground_truth': [entity],  # 简化的ground_truth
                'data_source': entity_data['source'],
                'prompt_id': prompt_id,
                # 'entity_idx': entity_idx,
                # 'question_idx': question_idx
            }
            if expand_mode:
                sample['orig_entity'] = pivot_entity 

            all_questions.append(sample)
    
    if n_questions is not None and training:
        all_questions = random.sample(all_questions, n_questions)

    if training:
        all_questions = sorted(all_questions, key=lambda x: x['priority'], reverse=True)
    
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

def convert_to_hdfs_format(questions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """转换为HDFS格式"""
    sessions = []
    
    # 从HDFS文件中动态读取system_prompt
    system_prompt = get_system_prompt_from_hdfs()

    for q in questions:
        prompt = q['prompt']
        if isinstance(prompt, list):
            prompt = prompt[0]['content']

        session = {
            'ability': 'ToB/Agent',
            'abtag_response': '{"task": ["NLP任务-ToB-Agent"], "constraint": []}',
            'add_turn': None,
            'create_time': None,
            'data_source': q['data_source'],
            'difficulty': None,
            'env_info': '',
            'ground_truth': json.dumps(q['ground_truth'], ensure_ascii=False),
            'instruct': '',
            'is_valid': True,
            'merge_source': q.get('merge_source', 'wxt_bc'),
            'message_id': None,
            'oj_feature': None,
            'old_ability': None,
            'origin_output': None,
            'prompt': [{'content': q['prompt'], 'role': 'user'}],
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
            'task_name': 'deep_research_official_rl'
        }

        sessions.append({'session': session})
    
    return sessions

def main():
    # 设置随机种子
    random.seed(42)
    
    if 1:
        # 1. 加载数据
        zh_data = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0628_zh_v2.json')
        en_data = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0629_en_v2.json')
        
        # 2. 提取实体和问题
        print("\n🔍 提取中文实体和问题 (前5k个实体)")
        zh_entities = extract_entities_and_questions(zh_data, 5000)
        for e in zh_entities:
            e['source'] = 'bc-syn-zh'
        
        print("\n🔍 提取英文实体和问题 (前10k个实体)")
        en_entities = extract_entities_and_questions(en_data, 10000)
        for e in en_entities:
            e['source'] = 'bc-syn-en'
        
        # 3. 合并中英文数据
        all_entities = zh_entities + en_entities
        print(f"\n📊 总实体数: {len(all_entities)}")
        
        # 4. 分割训练和测试
        print("\n✂️ 分割训练和测试数据 (80:20)")
        train_entities, test_entities = split_train_test(all_entities)
        test_entity_names = [e['entity'] for e in test_entities][:10]
        assert(test_entity_names == ['Jonas Reckermann', '危险湾', 'The Sweeney', '舔食者', 'Das kann ja heiter werden', 'James Joseph Dresnok', 'Pyt Kramer', 'Halleluja!', 'A Man Called Horse', '领袖'])
        
        # 5. 采样训练问题
        print("\n🎯 采样训练问题 (目标30k)")
        if 1:
            print('我们只采样1600个数据')
            import pdb; pdb.set_trace()
            train_questions = sample_questions(train_entities, rl_mode=True, n_questions=6400)
        else:
            train_questions = sample_questions(train_entities, rl_mode=True)
        test_questions = sample_questions(test_entities, training = False)

        if 0:
            import pdb; pdb.set_trace()
            random.shuffle(train_questions)
            train_questions[:len(train_questions)//2] = sorted(train_questions[:len(train_questions)//2], key=lambda x: x['priority'], reverse=True)
            train_sessions = convert_to_hdfs_format(train_questions)
            train_df = pd.DataFrame(train_sessions)
            
            train_file = '/root/wxt/bc/gen_questions/results/bc_train_30k_e2h_alphaseed_0714_6400.parquet'
            train_df.to_parquet(train_file, index=False)
            
            print(f"训练数据已保存: {train_file} ({len(train_df)} 条记录)")
            return 

        zh_data_2 = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0713_zh.json')
        en_data_2 = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0713_en.json')

        zh_entities_2 = extract_entities_and_questions(zh_data_2, 5000, expand_mode=True)
        en_entities_2 = extract_entities_and_questions(en_data_2, 10000, expand_mode=True)
        for e in zh_entities_2:
            e['source'] = 'bc-syn-expand-zh'
        
        for e in en_entities_2:
            e['source'] = 'bc-syn-expand-en'

        all_entities_2 = zh_entities_2 + en_entities_2
        print(f"\n📊 总实体数2: {len(all_entities_2)}")

        train_entities_2, test_entities_2 = split_train_test(all_entities_2)
        
        # 5. 采样训练问题
        print("\n🎯 采样训练问题 (目标30k)")
        train_questions2 = sample_questions(train_entities_2, expand_mode=True, n_questions=3200)
        
        # 6. 采样测试问题
        print("\n🎯 采样测试问题 (目标3k，每个实体1个)")
        test_questions2 = sample_questions(test_entities_2, training = False, expand_mode=True)


        # 7. 转换为HDFS格式
        print("\n🔄 转换为HDFS格式")
        train_questions = train_questions + train_questions2
        if 1:
            # 一半是从难到易，一半是随机
            random.shuffle(train_questions)
            train_questions[:len(train_questions)//2] = sorted(train_questions[:len(train_questions)//2], key=lambda x: x['priority'], reverse=True)
  
        test_questions = test_questions + test_questions2
        test_questions = random.sample(test_questions, 500)
        train_sessions = convert_to_hdfs_format(train_questions)
        test_sessions = convert_to_hdfs_format(test_questions)
        
        # 8. 保存数据
        print("\n💾 保存数据")
        
        train_df = pd.DataFrame(train_sessions)
        test_df = pd.DataFrame(test_sessions)
        
        train_file = '/root/wxt/bc/gen_questions/results/bc_train_30k_e2h_alphaseed_0714_single_multi_6400.parquet'
        test_file = '/root/wxt/bc/gen_questions/results/bc_test_3k_e2h_alphaseed_0714_single_multi_500.parquet'
        train_df.to_parquet(train_file, index=False)
        test_df.to_parquet(test_file, index=False)
        
        print(f"训练数据已保存: {train_file} ({len(train_df)} 条记录)")
        print(f"测试数据已保存: {test_file} ({len(test_df)} 条记录)")
        import pdb; pdb.set_trace()
    elif source == 'bc-syn':
        print("=" * 80)
        print("📊 创建BC问题训练数据集")
        print("=" * 80)
        
        # 1. 加载数据
        zh_data = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0628_zh_v2.json')
        en_data = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0629_en_v2.json')
        
        # 2. 提取实体和问题
        print("\n🔍 提取中文实体和问题 (前5k个实体)")
        zh_entities = extract_entities_and_questions(zh_data, 5000)
        for e in zh_entities:
            e['source'] = 'bc-syn-zh'
        
        print("\n🔍 提取英文实体和问题 (前10k个实体)")
        en_entities = extract_entities_and_questions(en_data, 10000)
        for e in en_entities:
            e['source'] = 'bc-syn-en'
        
        # 3. 合并中英文数据
        all_entities = zh_entities + en_entities
        print(f"\n📊 总实体数: {len(all_entities)}")
        
        # 4. 分割训练和测试
        print("\n✂️ 分割训练和测试数据 (80:20)")
        train_entities, test_entities = split_train_test(all_entities)
        test_entity_names = [e['entity'] for e in test_entities][:10]
        assert(test_entity_names == ['Jonas Reckermann', '危险湾', 'The Sweeney', '舔食者', 'Das kann ja heiter werden', 'James Joseph Dresnok', 'Pyt Kramer', 'Halleluja!', 'A Man Called Horse', '领袖'])
        
        # 5. 采样训练问题
        print("\n🎯 采样训练问题 (目标30k)")
        if 1:
            print('我们只采样3200个数据')
            import pdb; pdb.set_trace()
            train_questions = sample_questions(train_entities, n_questions=3200)
        else:
            train_questions = sample_questions(train_entities)
        
        # 6. 采样测试问题
        print("\n🎯 采样测试问题 (目标3k，每个实体1个)")
        test_questions = sample_questions(test_entities, training = False)
        
        # 7. 转换为HDFS格式
        print("\n🔄 转换为HDFS格式")
        train_sessions = convert_to_hdfs_format(train_questions)
        test_sessions = convert_to_hdfs_format(test_questions)
        
        # 8. 保存数据
        print("\n💾 保存数据")
        
        train_df = pd.DataFrame(train_sessions)
        test_df = pd.DataFrame(test_sessions)
        
        train_file = '/root/wxt/bc/gen_questions/results/bc_train_30k_e2h_alphaseed_0709.parquet'
        test_file = '/root/wxt/bc/gen_questions/results/bc_test_3k_e2h_alphaseed_0709.parquet'
        
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
    elif source == 'deep_researcher':
        # open /root/wxt/DeepResearcher/data/train.parquet
        with open('/root/wxt/DeepResearcher/data/train.parquet', 'rb') as f:
            df = pd.read_parquet(f)
        
        print(df.head())
        
        train_questions = df.to_dict(orient='records')
        new_questions = []
        for q in train_questions:
            new_questions.append({
                'data_source': q['data_source'],
                'ground_truth': q['reward_model']['ground_truth'],
                'prompt': q['prompt'][0]['content'].strip(), 
                'prompt_id': q['extra_info']['index']
            })
        train_questions = new_questions
        train_questions = random.sample(train_questions, 10000)
        print(f"采样了 {len(train_questions)} 个问题")
        train_sessions = convert_to_hdfs_format(train_questions)

        train_df = pd.DataFrame(train_sessions)
        train_file = '/root/wxt/bc/gen_questions/results/gair_dr_10k_alphaseed.parquet'
        print(f"保存数据到 {train_file}")
        train_df.to_parquet(train_file, index=False)

    elif source == 'web_walker':
        # open /root/wxt/DeepResearcher/data/train.parquet
        with open('/root/wxt/bc/webwalker.jsonl', 'r') as f:
            train_questions = [json.loads(line) for line in f]
        
        new_questions = []
        for i, q in enumerate(train_questions):
            new_questions.append({
                'data_source': 'web_walker',
                'ground_truth': q['answer'].strip(),
                'prompt': q['question'].strip(), 
                'prompt_id': 'web_walker_' + str(i)
            })
        train_questions = new_questions
        train_questions = random.sample(train_questions, 10000)
        print(f"采样了 {len(train_questions)} 个问题")
        train_sessions = convert_to_hdfs_format(train_questions)

        train_df = pd.DataFrame(train_sessions)
        train_file = '/root/wxt/bc/gen_questions/results/web_walker_10k_alphaseed.parquet'
        print(f"保存数据到 {train_file}")
        train_df.to_parquet(train_file, index=False)
    elif source == 'to_zhanghe':
        print("=" * 80)
        print("📊 创建BC问题训练数据集")
        print("=" * 80)
        
        # 1. 加载数据
        zh_data = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0628_zh_v2.json')
        en_data = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0629_en_v2.json')
        
        # 2. 提取实体和问题
        print("\n🔍 提取中文实体和问题 (前5k个实体)")
        zh_entities = extract_entities_and_questions(zh_data, 5000)
        for e in zh_entities:
            e['source'] = 'bc-syn-zh'
        
        print("\n🔍 提取英文实体和问题 (前10k个实体)")
        en_entities = extract_entities_and_questions(en_data, 10000)
        for e in en_entities:
            e['source'] = 'bc-syn-en'
        
        # 3. 合并中英文数据
        all_entities = zh_entities + en_entities
        print(f"\n📊 总实体数: {len(all_entities)}")
        
        # 4. 分割训练和测试
        print("\n✂️ 分割训练和测试数据 (80:20)")
        train_entities, test_entities = split_train_test(all_entities)
        all_entity_names = [e['entity'] for e in all_entities]
        test_entity_names = [e['entity'] for e in test_entities]
        assert(test_entity_names[:10] == ['Jonas Reckermann', '危险湾', 'The Sweeney', '舔食者', 'Das kann ja heiter werden', 'James Joseph Dresnok', 'Pyt Kramer', 'Halleluja!', 'A Man Called Horse', '领袖'])

        # 5. 采样训练问题
        print("\n🎯 采样训练问题 (目标30k)")
        train_questions = sample_questions(train_entities)
        
        # 6. 采样测试问题
        print("\n🎯 采样测试问题 (目标3k，每个实体1个)")
        test_questions = sample_questions(test_entities, training = False)

        #zh_data_2 = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0709_zh.json')
        #en_data_2 = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0709_en.json')
        zh_data_2 = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0713_zh.json')
        en_data_2 = load_json_data('/root/wxt/bc/gen_questions/results/bc_questions_0713_en.json')

        zh_entities_2 = extract_entities_and_questions(zh_data_2, 5000, expand_mode=True)
        en_entities_2 = extract_entities_and_questions(en_data_2, 10000, expand_mode=True)
        for e in zh_entities_2:
            e['source'] = 'bc-syn-expand-zh'
        
        for e in en_entities_2:
            e['source'] = 'bc-syn-expand-en'

        all_entities_2 = zh_entities_2 + en_entities_2
        print(f"\n📊 总实体数2: {len(all_entities_2)}")
        #test_entities_2 = [e for e in all_entities_2 if e['entity'] in test_entity_names]
        #train_entities_2 = [e for e in all_entities_2 if e['entity'] not in test_entity_names]
        train_entities_2, test_entities_2 = split_train_test(all_entities_2)
        #print(f"test_entity_names: {test_entity_names[:10]}")
        #print(f'Num of test_entities_2: {len(test_entities_2)}, train_entities_2: {len(train_entities_2)}')
        
        # 5. 采样训练问题
        print("\n🎯 采样训练问题 (目标30k)")
        train_questions2 = sample_questions(train_entities_2, expand_mode=True)
        
        # 6. 采样测试问题
        print("\n🎯 采样测试问题 (目标3k，每个实体1个)")
        test_questions2 = sample_questions(test_entities_2, training = False, expand_mode=True)
        
        #train_questions = train_questions + train_questions2
        #test_questions = test_questions + test_questions2
        
        # 7. 转换为HDFS格式
        print("\n🔄 转换为HDFS格式")
        train_sessions = convert_to_hdfs_format(train_questions)
        test_sessions = convert_to_hdfs_format(test_questions)
        
        # 8. 保存数据
        print("\n💾 保存数据")
        
        train_df = pd.DataFrame(train_sessions)
        test_df = pd.DataFrame(test_sessions)
        
        train_file = '/root/wxt/bc/gen_questions/results/bc_train_30k_e2h_alphaseed_v2_0712_part_single.parquet'
        test_file = '/root/wxt/bc/gen_questions/results/bc_test_3k_e2h_alphaseed_v2_0712_part_single.parquet'

        train_df.to_parquet(train_file, index=False)
        test_df.to_parquet(test_file, index=False)
        
        # 9. 把第二份数据保存
        print("\n🔄 转换为HDFS格式（第二次）")
        import pdb; pdb.set_trace()
        train_sessions = convert_to_hdfs_format(train_questions2)
                
        test_sessions = convert_to_hdfs_format(test_questions2)
        
        print("\n💾 保存数据")
        
        train_df = pd.DataFrame(train_sessions)
        test_df = pd.DataFrame(test_sessions)
        
        train_file = '/root/wxt/bc/gen_questions/results/bc_train_30k_e2h_alphaseed_v2_0713_part_multi.parquet'
        test_file = '/root/wxt/bc/gen_questions/results/bc_test_3k_e2h_alphaseed_v2_0713_part_multi.parquet'

        train_df.to_parquet(train_file, index=False)
        test_df.to_parquet(test_file, index=False)

        if 1: # 给陈博宇抽检用
            
            # Export random 100 data points to Excel with Chinese translations
            print("\n📊 Exporting sample data to Excel...")
            
            # Check if openpyxl is available for Excel export
            try:
                import openpyxl
                excel_available = True
            except ImportError:
                print("Warning: openpyxl not available, will export as CSV instead")
                excel_available = False
            
            # Sample 100 random data points from train_questions2
            sample_data = random.sample(train_questions2, min(100, len(train_questions2)))
            import pdb; pdb.set_trace()
            # Sort by priority (descending order for highest priority first)
            sample_data = sorted(sample_data, key=lambda x: x['priority'], reverse=True)
            
            # Prepare data for Excel export
            excel_data = []
            for i, item in enumerate(sample_data):
                excel_data.append({
                    'answer': item['entity'],
                    'answer_type': item['entity_type'],
                    'popularity': item['priority'],  # Renamed from priority
                    'question': item['question'],
                })
            
            # Create DataFrame with Chinese headers and export
            df = pd.DataFrame(excel_data)
            
            # Rename columns to Chinese headers
            df.columns = ['答案', '答案类型', '知名度', '问题']
            
            output_filename = '/root/wxt/bc/gen_questions/results/multi_entity_questions_samples_0713.xlsx'
            df.to_excel(output_filename, index=False, engine='openpyxl')
            print(f"✅ Excel file exported: {output_filename}")
            
            
            print(f"   Contains {len(excel_data)} data points sorted by popularity")

        
        
        
if __name__ == "__main__":
    main()
