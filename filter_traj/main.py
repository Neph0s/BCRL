import glob 
import json 
import pandas as pd 
import argparse 
import os
from concurrent.futures import ThreadPoolExecutor
import threading 

MAX_LENGTH = 32768
def is_mostly_chinese(x: str) -> bool:
    """
    判断字符串中中文汉字数量是否超过英文单词数量的一半
    
    参数:
    x (str): 输入的字符串
    
    返回:
    bool: 如果汉字数量 > 单词数量/2，返回True，否则返回False
    """
    import re
    
    # 计算中文字符数量
    chinese_count = len(re.findall(r'[\u4e00-\u9fff]', x))
    
    # 计算英文单词数量（由连续字母组成的序列）
    english_words = re.findall(r'[a-zA-Z]+', x)
    english_count = len(english_words)
    
    #print(f'Num Chinese {chinese_count} English {english_count}')
    # 如果没有英文单词，避免除以零
    if english_count == 0:
        return chinese_count > 0
    
    # 判断汉字数量是否超过英文单词数量的一半
    return chinese_count > (english_count / 2)

parser = argparse.ArgumentParser(description="可视化parquet文件数据")
parser.add_argument("file_path", help="parquet文件路径")

args = parser.parse_args()

traj_path = args.file_path 
#'/root/wxt/traj_zhanghe/output_filter_pipeline_input_ww_dr_mix_v1_sp_v13_attc'
#'/root/wxt/traj_zhanghe/output_filter_pipeline_input_xintao_reverse_v1_sp_v13_attc'
#'/Users/bytedance/sft_traj/filter_pipeline_input_webgptv3_from_zehui'
output_traj_path = f'./filtered_traj/{traj_path.split("/")[-1]}-{MAX_LENGTH}-refined_v2.json'

# 查找函数（支持通配符）
def find_local_files_glob(local_path, pattern):
    return (glob.glob(f"{local_path}/**/{pattern}", recursive=True))

# 查找目标文件
case_result_json_files = find_local_files_glob(traj_path, 'case_result.json')
parquet_files = find_local_files_glob(traj_path, '*.parquet')

print(f'Num Case Result Files {len(case_result_json_files)}')

all_messages = []
all_traj_infos = []

# Thread-safe locks for shared data
messages_lock = threading.Lock()
traj_infos_lock = threading.Lock()

def process_case_result(case_result_file):
    with open(case_result_file, 'r') as f:
        case_result = json.load(f)
    
    # 如果是case_result.json文件，如 /root/wxt/traj_old/output_filter_pipeline_input_xintao_reverse_v1_sp_v13_attc/00/38/2984/case_result.json
    # step 1：找到 "final_sample_idx"，读取

    final_sample_idx = case_result.get('final_sample_idx')

    # step 2: 如果"final_sample_idx"不是None，说明有需要的sample，读取case_result_file.replace('case_result', f'{final_sample_idx}_agent')
    if final_sample_idx is None: return 

    traj_infos = case_result.get('traj_infos', {})
    traj_infos = {_['sample_idx']: _ for _ in traj_infos}[final_sample_idx]

    if traj_infos['num_tokens_all'] > MAX_LENGTH or traj_infos['num_tool_calls'] < 3: 
        return 

    message_file = case_result_file.replace('case_result', f'{final_sample_idx}_agent')
    with open(message_file, 'r') as f:
        message_data = json.load(f)
    
    messages = []
    query = message_data[1]['content']
    if is_mostly_chinese(query):
        language = 'zh'
    else:
        language = 'en'

    sys_prompt = 'You are an online information search expert. Your task is to collect relevant information through online searches based on user questions and then answer those questions using the gathered information.\n\n# Task Description\nUpon receiving a user\'s question, you need to fully understand their needs, utilize the retrieval tools I provide, and analyze the corresponding information and data from multiple perspectives to answer the user\'s question.\n\nThe following principles must be adhered to during task execution:\n- **Fully Understand User Needs**: You must analyze the user\'s question from multiple angles, breaking it down if necessary, to ensure you grasp the primary intent of the question.\n- **Flexible Use of Tools**: Once you fully understand the user\'s needs, please use the tools I provide to retrieve information.\n    - If you reflect and find that the information previously obtained by the tool is incomplete or incorrect, making it insufficient to answer the user\'s question, please consider what additional information needs to be searched, whether the approach needs adjustment, and call the tool again to obtain complete information.\n    - You can also break down the information to be retrieved into multiple sub-questions, each specific enough to be retrieved independently.\n- **Diverse Answering Style**: You need to combine the acquired information to ensure your answers are concise and accurate.\n\nAdditionally, you possess the ability for deep thinking. Before replying to the user, you will engage in comprehensive deep thought. Your thought process should be enclosed within `<think>` and `</think>` tags, and based on this thought process, you will either trigger tool calls to retrieve information or formulate the final answer.\nFor example: "<think>This is the thinking process...</think>This is the tool call" or "<think>This is the thinking process...</think>This is the final answer."\nFor all user requests, you must always think deeply before answering.\n\nYou can trigger multiple tool calls each time.\n\nWhen using tools, strictly follow this format:\n<|FunctionCallBegin|>[{"name": "function_name","parameters": {"param": "xxx"}}, {"name": "function_name","parameters": {"param": "xxx"}}, ...]<|FunctionCallEnd|>\n\n## Environment Information\nCurrent Location: Beijing\nCurrent Time: {2025-07-01 03:37:17 PM JST}name=functions\n[{"name": "search_bing", "description": "Bing Web Search API provides secure, ad-free, and location-aware search results that can extract relevant information from billions of web documents. With a single API call, you can leverage Bing\'s capabilities to search billions of web pages, images, videos, and news, helping your users find the content they need from the World Wide Web.", "parameters": {"type": "object", "properties": {"query": {"type": "string", "description": "Keywords for internet search. Use either the same language as the user\'s question, or English."}, "offset": {"type": "integer", "description": "Offset starting from 0, not exceeding 100", "default": 0}, "count": {"type": "integer", "description": "Number of results per page, maximum 20, default 10", "default": 10}, "mkt": {"type": "string", "description": "Market codes, use en-US for English content search, use zh-CN for Chinese content search, default en-US", "default": "en-US"}}, "required": ["query"]}}, {"name": "read_beautiful_soup", "description": "Use requests + BeautifulSoup to get the raw content of a webpage. If the raw content is too long, it will automatically trigger model summarization.", "parameters": {"type": "object", "properties": {"url": {"type": "string", "description": "The webpage URL to read"}, "title": {"description": "Webpage title, optional", "type": "string"}, "length_limit": {"type": "integer", "description": "If the page content character count exceeds this value, the large model will summarize the webpage content, default 2000", "default": 2000}, "query": {"type": "string", "description": "Used for targeted summarization by the large model, default value is \'None\'.", "default": "None"}}, "required": ["url"]}}]'

    for i, m in enumerate(message_data):
        if m['role'] == 'system':
            m['content'] = sys_prompt
            messages.append({'role': 'system', 'content': m['content'], 'loss_mask': 0.0, 'name': ''})
        elif m['role'] == 'user':
            messages.append({'role': 'user', 'content': m['content'], 'loss_mask': 0.0, 'name': ''})
        elif m['role'] == 'assistant':
            if 'tool_calls' in m and m['tool_calls'] is not None:
                assert i != len(message_data) - 1 
                m['content'] = m['content'] 
                tool_calls = []
                
                tool_call_records = {}

                for tool_call in m['tool_calls']:
                    tool_call_ = {"name": tool_call['function']["name"], "parameters": eval(tool_call['function']["arguments"])} 
                    tool_calls.append(tool_call_)
                    tool_call_records[tool_call['id']] = tool_call_
                
                m['content'] += '<|FunctionCallBegin|>' + json.dumps(tool_calls, ensure_ascii=False) + '<|FunctionCallEnd|>'
            else:
                assert i == len(message_data) - 1 # 最后一条
                m['content'] = '<think>' + m['reasoning_content'] + '</think>' + m['content']
            messages.append({'role': 'assistant', 'content': m['content'], 'loss_mask': 1.0, 'name': ''})
        elif m['role'] == 'tool': 
            tool_message = m['content']

            orig_tool_message = tool_message
            import re
            tool_message = re.sub(r'(\[发布时间\] \d{4})年(\d{1,2})月(\d{1,2})日', r'\1-\2-\3', tool_message)
            tool_message = tool_message.replace("[发布时间] 无", "[publish_time] None")
            for zh_word, en_word in [('[摘要]', '[summary]'), ('[标题]', '[title]'), ('[序号]', '[number]'), ('[发布时间]', '[publish_time]'), ('[来源]', '[source]'), ('（星期一）', '(Monday)'), ('（星期二）', '(Tuesday)'), ('（星期三）', '(Wednesday)'), ('（星期四）', '(Thursday)'), ('（星期五）', '(Friday)'), ('（星期六）', '(Saturday)'), ('（星期日）', '(Sunday)'),]:
                tool_message = tool_message.replace(zh_word, en_word)
            
            tool_input = tool_call_records[m['tool_call_id']]
            tool_message = 'tool call: ' + json.dumps(tool_input, ensure_ascii=False)  + '\n' + tool_message

            messages.append({'role': 'tool', 'content': tool_message, 'loss_mask': 0.0, 'name': tool_input['name']})
    
    # 对第一条thinking做处理
    assert(messages[2]['role'] == 'assistant')
    q = messages[1]['content']
    try:
        thinking_content, tool_call = messages[2]['content'].split('</think>')
        thinking_content = thinking_content.replace('<think>', '')
    except:
        return 

    sys_prompt = """Your task is to infer a plausible {thinking process} that connects a given {question} to a {tool_call}. The inferred process must meet the following criteria:
- Logical Coherence: It must clearly and logically explain the reasoning that leads from the {question} to the specific {tool_call}.
- Factual Minimality: It should only contain the minimal factual information necessary to justify the {tool_call}. Avoid introducing external knowledge about real-world people, events, or artifacts, though commonsense knowledge is permitted.
- Language: It must be written in the same language as the {question}.
- Don't include tool calls in the {thinking process}.
    
## Input
===question===
{question_}
===tool_call===
{tool_call_}

## Output in the following format
Analysis: {your analysis} 
Thinking Process: {the inferred thinking process}
""".replace('{question_}', q).replace('{tool_call_}', tool_call)

    from utils import get_response

    def ensure_format(response, **kwargs):
        if len(response.split('Thinking Process:')) == 2:
            return response
        else:
            return False

    response = get_response([ensure_format], model='seed', messages=[{'role': 'user', 'content': sys_prompt}])
    refined_thinking_process = response.split('Thinking Process:')[1].strip(' ')
    
    if '===tool_call===' in refined_thinking_process:
        refined_thinking_process = refined_thinking_process.split('===tool_call===')[0]
    elif  '<|FunctionCallBegin|>' in refined_thinking_process:
        refined_thinking_process = refined_thinking_process.split('<|FunctionCallBegin|>')[0]

    all_thoughts = [ m['content'].split('</think>')[0].replace('<think>', '') for m in messages if m['role'] == 'assistant' and '<think>' in m['content']]

    assert (len(all_thoughts) > 1)
    subsequent_thoughts_str = '\n\nAction: ...\n\nObservation: ...\n\n\n'.join([ f'Step {i+2}:\nThought: ' + t for i, t in enumerate(all_thoughts[1:4])])

    sys_prompt = """Your task is to check if a proposed {new first thought} is consistent with reasoning steps mentioned later, and enrich it if necessary.

Context: You are given a multi-step reasoning process where the original first thought was lost and replaced by a concise {new first thought}. Your goal is to ensure this replacement doesn't omit any critical conclusions that later steps rely on.

Your process should be:

1. Identify Inherited Reasoning: Your main goal is to scan the {subsequent thoughts} for phrases that explicitly refer back to a previous step, like "Following the previous reasoning," "Based on the initial conclusion," or similar patterns (e.g., "根据之前的思路" in Chinese). The specific conclusion or reasoning attached to these phrases is the "inherited reasoning". Crucial Example:
    - If a {subsequent thought} says: "Based on the previous reasoning, the most likely candidate is ENTITY_A."
    - The "inherited reasoning" you must identify is the entire proposition: "The most likely candidate was ENTITY_A."
2. Check for Omission: Verify if this "inherited reasoning" (e.g., the conclusion that "ENTITY_A was the most likely candidate") is already present or implied in the {new first thought}.
3. Enrich if Necessary: If the "inherited reasoning" is missing, integrate it into the {new first thought}. Make minimum yet necessary changes to ensure the thought is logically complete. For example, if the "inherited reasoning" is "The most likely candidate was ENTITY_A", you should add discussion about why ENTITY_A may be related to the answer of the {question}.  

The final {enriched new first thought} must adhere to the following principles:
- Logical Coherence: It must clearly and logically explain the reasoning that leads from the {question} to the subsequent actions, including {first tool call} and {subsequent thoughts}.
- Minimal Enrichment: Your changes to the original {new first thought} should be as minimal as possible. 
- Language: It must be written in the same language as the {question}.
- Don't include tool calls in the {thinking process}.


## Input
===question===
{question_}
===new first thought===
{new_first_thought_}
===first tool call===
{first_tool_call_}
===subsequent thoughts===
{subsequent_thoughts_}

## Output in the following format
Analysis: {your analysis, following the above instructions} 
Need Enrichment: {True of False}
Enriched Thought: {The final, complete version of the thought. If no changes were needed, this will be identical to the original new first thought. in {language}}
""".replace('{question_}', q).replace('{new_first_thought_}', refined_thinking_process).replace('{first_tool_call_}', tool_call).replace('{subsequent_thoughts_}', subsequent_thoughts_str).replace('{language}', language)


    def ensure_format2(response, **kwargs):
        if len(response.split('Enriched Thought:')) == 2 and 'Need Enrichment:' in response:
            return response
        else:
            return False

    print('\n\n=====Original Thinking Process=====\n\n', thinking_content)
    print('\n\n=====Original Subsequent Thoughts=====\n\n', subsequent_thoughts_str)
    print('\n\n=====Refined Thinking Process=====\n\n', refined_thinking_process)

    response = get_response([ensure_format2], model='claude-4-sonnet', messages=[{'role': 'user', 'content': sys_prompt}])
    if response is None:
        return 

    _, enriched_thinking_process = response.split('Enriched Thought:')
    need_enrichment = _.split('Need Enrichment:')[-1].strip(' \n').lower()
    enriched_thinking_process = enriched_thinking_process.strip(' ')

    if need_enrichment != 'true':
        enriched_thinking_process = refined_thinking_process

    if '===tool_call===' in enriched_thinking_process:
        enriched_thinking_process = enriched_thinking_process.split('===tool_call===')[0]
    elif  '<|FunctionCallBegin|>' in enriched_thinking_process:
        enriched_thinking_process = enriched_thinking_process.split('<|FunctionCallBegin|>')[0]

    final_assistant_message = '<think>' + enriched_thinking_process + '</think>' + tool_call

    print('\n\n=====Final Thinking Process=====\n\n', final_assistant_message)
    print('\n\n=====Full Enrich Response=====\n\n', response)
    messages[2]['content'] = final_assistant_message

    
    #print(json.dumps(messages, ensure_ascii=False,indent=2))
    

    return messages, traj_infos


# Use ThreadPoolExecutor to process files in parallel
if 1:
    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = []
        for j, case_result_file in enumerate(case_result_json_files):
            future = executor.submit(process_case_result, case_result_file)
            futures.append(future)
        
        # Wait for all tasks to complete and collect results
        for future in futures:
            result = future.result()
            if result is not None:
                messages, traj_infos = result
                all_messages.append({'messages': messages})
                all_traj_infos.append(traj_infos)
else:
    # not parallel 
    for j, case_result_file in enumerate(case_result_json_files):
        result = process_case_result(case_result_file)
        if result is not None:
            messages, traj_infos = result
            all_messages.append({'messages': messages})
            all_traj_infos.append(traj_infos)
    

with open(output_traj_path, 'w') as f:
    json.dump(all_messages, f, ensure_ascii=False, indent=2)

df = pd.DataFrame(all_messages)
df.to_parquet(output_traj_path.replace('.json', '.parquet'))

# 统计traj_infos
print('total num: ', len(all_traj_infos))
if all_traj_infos:
    for k in all_traj_infos[0].keys():
        all_values = [_.get(k) for _ in all_traj_infos]
        max_value = max(all_values)
        avg_value = sum(all_values) / len(all_values)
        min_value = min(all_values)
        print(f'{k} max: {max_value} avg: {avg_value} min: {min_value}')



