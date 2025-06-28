import json
import requests
import time
import os
import pandas as pd
import pdb
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import get_response, save_result, save_result_txt, extract_json, ensure_question_format
import random 
from utils import set_cache_path

# 配置方法选择
search_model = 'gemini_search'  # 选择 'gemini_search' 或 'deer-flow'
question_model = 'claude-4-sonnet' #'gpt'
language = 'en'  # 选择 'zh' 或 'en'
entity_files = ['my_entities_en.csv', 'wikidata_entities_with_popularity_en_0625.csv'] 
output_file = 'bc_questions_0627_en.json'
existing_files = ["results/bc_questions_0625_en.json"]  # 已有的数据文件
parallel = True
set_cache_path('.cache-bc_questions_0625_en.pkl') # '.cache-' + output_file.replace('.json', '.pkl'))

progress_count = 0
progress_lock = threading.Lock()
total_entities = 0
save_interval = 100  # 每100个实体保存一次

def to_my_entity_key(entity_info):
	if 'entity_info' in entity_info:
		return entity_info['entity_info']['label'] + '[WIKI:' +str(entity_info['entity_info']['id']) + ']'
	else:
		return  entity_info['label'] + '[WIKI:' +str(entity_info['id']) + ']'

def save_progress(results, filename=None):
	"""统一的保存进度函数"""
	if filename is None:
		filename = output_file
	
	os.makedirs("results", exist_ok=True)
	with open(f'results/{filename}', 'w', encoding='utf-8') as f:
		json.dump(results, f, ensure_ascii=False, indent=2)

def process_entity(entity_info):
	"""处理单个实体的函数，用于并发执行"""
	global progress_count, total_entities
	
	entity_name = entity_info['label']
	entity_description = entity_info.get('description', '')
	
	with progress_lock:
		progress_count += 1
		current = progress_count
	
	print(f"[{current}/{total_entities}] 开始查询实体: {entity_name}")
	#import pdb; pdb.set_trace()
	# 保存完整的实体信息
	result = {
		'entity': entity_name,
		'entity_info': entity_info.to_dict() if hasattr(entity_info, 'to_dict') else entity_info
	} 
	
	from prompts import get_prompt

	messages = []

	# 搜集实体信息 - 第一次使用label + description
	search_prompt = get_prompt('search_prompt', language)
	entity_full = f"{entity_name}({entity_description})" if entity_description else entity_name
	prompt = search_prompt.replace('{entity}', entity_full, 1).replace('{entity}', entity_name)
	messages.append({'role': 'user', 'content': prompt})
	knowledge = get_response(model=search_model, messages=messages)
	result['search_response'] = knowledge
	messages.append({'role': 'assistant', 'content': knowledge})

	# 二次扩展
	search_second_prompt = get_prompt('search_second_prompt', language)
	messages.append({'role': 'user', 'content': search_second_prompt})
	knowledge2 = get_response(model=search_model, messages=messages)
	result['search_again_response'] = knowledge2
	messages.append({'role': 'assistant', 'content': knowledge2})
	
	# 生成问题 - 后续只使用label
	# question_generate_prompt = get_prompt('question_generate_prompt', language)
	# for N_I_LOW, N_I_HIGH in [(3, 4), (5, 6)]:
	# 	prompt = question_generate_prompt.replace('{entity}', entity_name).replace('{N_I_LOW}', str(N_I_LOW)).replace('{N_I_HIGH}', str(N_I_HIGH)).replace('{N_Q}', '3')

	# 	messages.append({'role': 'user', 'content': prompt})
	# 	response = get_response([extract_json, ensure_question_format], model=question_model, messages=messages)

	# 	if 'question_response' not in result:
	# 		result['question_response'] = []
		
	# 	result['question_response'].append(response)
		
	# 	messages.pop()

	# 	# 离线判定答案唯一性 TODO

	return result


def main():
	global total_entities
	
	# 先读取已有结果文件
	existing_results = {}
	existing_entitiy_keys = set()
	
	for existing_file in existing_files:
		if os.path.exists(existing_file):
			print(f"读取已有结果文件: {existing_file}")
			with open(existing_file, 'r', encoding='utf-8') as f:
				existing_data = json.load(f)
				# 转换旧格式key为新格式
				for key, value in existing_data.items():
					if 'WIKI:' in key:
						new_key = key
					else:
						new_key = to_my_entity_key(value)
					existing_results[new_key] = value
			print(f"已读取 {len(existing_data)} 个已有实体")
		else:
			print(f"⚠️ 文件不存在: {existing_file}")
	
	existing_entitiy_keys = set(existing_results.keys())
	print(f"总共已有 {len(existing_entitiy_keys)} 个实体")
	
	# 读取多个实体文件
	entities_data = []
	import pdb; pdb.set_trace()
	n_entities = 10100
	# 按优先级顺序读取各个文件
	for i_f, entity_file in enumerate(entity_files):
		df = pd.read_csv(entity_file)
		print(f"成功读取 {entity_file}，共 {len(df)} 条记录")
		
		# 读取实体信息，添加到列表中，排除已有实体
		_entities_data = []
		for _, row in df.iterrows():
			entity_dict = row.to_dict()
			# 只添加不在已有实体中的新实体
			if to_my_entity_key(entity_dict) not in existing_entitiy_keys:
				_entities_data.append(entity_dict)
		
		print(f"排除已有实体后，剩余 {len(_entities_data)} 个新实体")

		# 0627:过滤，仅保留popularity_score < 10000的实体	
		_entities_data = [entity_dict for entity_dict in _entities_data if entity_dict['popularity_score'] < 10000]
		print(f"按entity_info['popularity_score'] < 10000过滤后，剩余 {len(_entities_data)} 个新实体")

		assert i_f < 2
		if i_f == 1:
			n_sample = n_entities - len(existing_entitiy_keys) - len(entities_data)
			from utils import stable_shuffle
			entities_data.extend(stable_shuffle(_entities_data)[:n_sample])
		else:
			entities_data.extend(_entities_data)
	# 展示popularity分布
	print(f"总共读取了 {len(entities_data)} 个实体")
	
	if 1:
		# 提取popularity信息并展示分布
		popularities = []
		for entity in entities_data:
			if 'popularity_score' in entity and entity['popularity_score'] is not None:
				score = float(entity['popularity_score'])
				import math 
				if math.isnan(score): score = 0
				
				popularities.append(score)
		
		if popularities:
			import numpy as np
			print(f"\n=== Popularity分布统计 ===")
			print(f"有效popularity记录数: {len(popularities)}")
			print(f"最小值: {np.min(popularities):.4f}")
			print(f"最大值: {np.max(popularities):.4f}")
			print(f"平均值: {np.mean(popularities):.4f}")
			print(f"中位数: {np.median(popularities):.4f}")
			print(f"标准差: {np.std(popularities):.4f}")
			
			# 显示分位数
			percentiles = [10, 25, 50, 75, 90, 95, 99]
			print(f"\n分位数分布:")
			for p in percentiles:
				print(f"  {p}%: {np.percentile(popularities, p):.4f}")
			
			# 显示分布区间统计
			bins = [0, 0.001, 0.01, 0.1, 1.0, float('inf')]
			bin_labels = ['<0.001', '0.001-0.01', '0.01-0.1', '0.1-1.0', '>=1.0']
			print(f"\n分布区间统计:")
			for i in range(len(bins)-1):
				count = sum(1 for p in popularities if bins[i] <= p < bins[i+1])
				percentage = count / len(popularities) * 100
				print(f"  {bin_labels[i]}: {count} ({percentage:.1f}%)")
		else:
			print("未找到有效的popularity数据")
	
	
	total_entities = len(entities_data)

	# 根据方法调整并发数
	
	# 初始化结果字典，包含已有结果和新实体
	results = existing_results.copy()  # 先复制已有结果

	if parallel:
		max_workers = 15
		completed_count = 0
		
		with ThreadPoolExecutor(max_workers=max_workers) as executor:
			# 提交所有任务
			future_to_entity = {executor.submit(process_entity, entity_info): entity_info['label'] for entity_info in entities_data}
			
			# 处理完成的任务
			for future in as_completed(future_to_entity):
				entity_name = future_to_entity[future]
				result = future.result()
				results[to_my_entity_key(result)] = result
				completed_count += 1
				
				# 每100个实体保存一次
				if completed_count % save_interval == 0:
					print(f"💾 已完成 {completed_count} 个实体，保存中间结果...")
					save_progress(results)
					print(f"💾 中间结果已保存: results/{output_file}")

	else:
		for i, entity_info in enumerate(entities_data, 1):
			result = process_entity(entity_info)
			results[to_my_entity_key(result)] = result
			
			# 每100个实体保存一次
			if i % save_interval == 0:
				print(f"💾 已完成 {i} 个实体，保存中间结果...")
				save_progress(results)
				print(f"💾 中间结果已保存: results/{output_file}")

	# 统计结果：已有数据 + 新完成的数据
	total_completed = len([result for result in results.values() if result is not None])
	new_completed = len([result for entity_info in entities_data for result in [results[to_my_entity_key(entity_info)]] if result is not None])
	new_failed = len(entities_data) - new_completed
	
	print(f"📊 统计结果:")
	print(f"  已有实体: {len(existing_entitiy_keys)}")
	print(f"  新处理实体: {len(entities_data)}")
	print(f"  新成功: {new_completed}, 新失败: {new_failed}")
	print(f"  总计成功: {total_completed}, 总计实体: {len(results)}")

	# 保存汇总结果
	save_progress(results, output_file)

	# 保存TXT格式的简化结果
	save_result_txt(f'results/{output_file}_simple.txt', results)

	print(f"📁 结果保存在 results/ 目录下（使用{question_model}方法）")
	print(f"📄 JSON格式: results/{output_file}.json")
	print(f"📄 TXT格式: results/{output_file}_simple.txt")

def print_questions():
	with open('results/claude-4-sonnet.json', 'r', encoding='utf-8') as f:
		results = json.load(f)

	simple_results = {entity: results[entity.split('[WIKI:')[0]]['question_response'] for entity in results}
	# for entity, result in results.items():
	#     print(entity)
	#     for question in result['question_response']:
	#         print(json.dumps(question, indent=4, ensure_ascii=False))

	with open('results/bc_questions_0625_zh.json', 'w', encoding='utf-8') as f:
		json.dump(simple_results, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
	main()
	#print_questions()