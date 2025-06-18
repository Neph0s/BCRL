import pdb 
import os
import re 
import random 
import openai
import json
import logging
import time  
import jsonlines 
import requests 
import io
import pickle
import random
import __main__
import tiktoken
from typing import Dict, List

with open('config.json', 'r') as f:
	config = json.load(f)

streaming = False

def setup_logger(name, log_file, level=logging.INFO, quiet=False):
	logger = logging.getLogger(name)
	logger.setLevel(level)

	if logger.hasHandlers():
		logger.handlers.clear()

	file_handler = logging.FileHandler(log_file, encoding='utf-8')
	file_handler.setLevel(logging.DEBUG)
	file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	file_handler.setFormatter(file_formatter)
	logger.addHandler(file_handler)

	if not quiet:
		console_handler = logging.StreamHandler()
		console_handler.setLevel(level)
		console_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s [%(filename)s:%(lineno)d]')
		console_handler.setFormatter(console_formatter)
		logger.addHandler(console_handler)

	return logger

logger = setup_logger(__name__, f'{__file__.split(".")[0]}.log', level=logging.INFO, quiet=False)

from contextlib import contextmanager
import tempfile
@contextmanager
def _tempfile(dir=None,*args, **kws):
	""" Context for temporary file.
	Will find a free temporary filename upon entering
	and will try to delete the file on leaving
	Parameters
	----------
	suffix : string
		optional file suffix
	dir : string
		directory to create temp file in, will be created if doesn't exist
	"""
	if dir is not None:
		os.makedirs(dir, exist_ok=True)
		
	fd, name = tempfile.mkstemp(dir=dir, *args, **kws)
	os.close(fd)
	try:
		yield name
	finally:
		try:
			os.remove(name)
		except OSError as e:
			if e.errno == 2:
				pass
			else:
				raise e
			
@contextmanager
def open_atomic(filepath, *args, **kwargs):
	""" Open temporary file object that atomically moves to destination upon
	exiting.
	Allows reading and writing to and from the same filename.
	Parameters
	----------
	filepath : string
		the file path to be opened
	fsync : bool
		whether to force write the file to disk
	kwargs : mixed
		Any valid keyword arguments for :code:`open`
	"""
	fsync = kwargs.pop('fsync', False)

	original_permissions = os.stat(filepath).st_mode if os.path.exists(filepath) else None 

	with _tempfile(dir=os.path.join(os.path.dirname(filepath), 'temp')) as tmppath:
		with open(tmppath, *args, **kwargs) as f:
			yield f
			if fsync:
				f.flush()
				os.fsync(f.fileno())
		os.rename(tmppath, filepath)
		if original_permissions is not None:
			os.chmod(filepath, original_permissions)

import datetime
def convert_to_timestamp(time_str: str):
	return time.mktime(datetime.datetime.strptime(time_str, "%Y-%m-%d").timetuple())

def safe_pickle_dump(obj, fname):
	"""
	prevents a case where one process could be writing a pickle file
	while another process is reading it, causing a crash. the solution
	is to write the pickle file to a temporary file and then move it.
	"""
	with open_atomic(fname, 'wb') as f:
		pickle.dump(obj, f, -1) # -1 specifies highest binary protocol


ERROR_SIGN = '[ERROR]'

cache_path = config['cache']['default_path']
cache_sign = True
cache = None
reload_cache = False

def set_cache_path(new_cache_path):
	global cache_path
	cache_path = new_cache_path
	global reload_cache
	reload_cache = True

def cached(func):
	def wrapper(*args, **kwargs):		
		# extract_from_chunk 
		if func.__name__ == 'extract_from_chunk':
			key = ( func.__name__, args[0]['title'], args[1]) 
		else:
			key = ( func.__name__, str(args), str(kwargs.items())) 

		global cache
		global reload_cache

		if reload_cache:
			cache = None # to reload
			reload_cache = False

		if cache == None:
			if not os.path.exists(cache_path):
				cache = {}
			else:
				try:
					cache = pickle.load(open(cache_path, 'rb'))  
				except Exception as e:
					# logger.info cache_path and throw error
					logger.error(f'Error loading cache from {cache_path}')
					cache = {}

		if (cache_sign and key in cache) and not (cache[key] is None):
			return cache[key]
		else:		
			result = func(*args, **kwargs)
			if result != None:
				cache[key] = result
				safe_pickle_dump(cache, cache_path)
			return result

	return wrapper

enc = tiktoken.get_encoding(config['encoding']['name'])

def encode(text):
	return enc.encode(text)

def decode(tokens):
	return enc.decode(tokens)

def num_tokens_from_string(string: str, encoding_name: str = "cl100k_base") -> int:
	encoding = tiktoken.get_encoding(encoding_name)
	num_tokens = len(encoding.encode(string))
	logger.info(f"Number of tokens: {num_tokens}")
	return num_tokens

def gemini(messages, search=False):
	"""使用现有的gemini search API"""
	# 从配置文件获取API配置
	gemini_config = config['gemini_search']
	url = gemini_config['url']
	params = {
		"ak": gemini_config['ak']
	}
	
	# 请求头
	headers = {
		"Content-Type": "application/json",
		"X-TT-LOGID": gemini_config['log_id']
	}
	
	# 请求数据
	model = gemini_config['model']
	data = {
		"model": model, 
		"messages": messages,
		"thinking": {
			"include_thoughts": True
		},
		"stream": False,
	}

	if search:
		data['tools'] = [
			{
				"type": "google_search"
			}
		]
	
	try:
		response = requests.post(
			url=url,
			params=params,
			headers=headers,
			json=data,
			timeout=gemini_config['timeout']
		)

		return response.json()['choices'][0]['message']['content']
			
	except Exception as e:
		print(f"请求失败: {e}")
		return None

def claude(messages):
	"""使用现有的claude API"""
	# 从配置文件获取API配置
	claude_config = config['claude']
	
	# 请求数据
	client = openai.AzureOpenAI(
		azure_endpoint=claude_config['url'],
		api_version=claude_config['api_version'],
		api_key=claude_config['ak'],
	)

	try:
		response = client.chat.completions.create(
			model=claude_config['model'],
			messages=messages, 
			extra_headers={"X-TT-LOGID": claude_config['log_id']},  
			#如果改模型需要thinking
			max_tokens=4096,
			extra_body={
				"thinking": {
					"type": "enabled",
					"budget_tokens": 2000,
				}
			}
		)

		return response.choices[0].message.content
			
	except Exception as e:
		time.sleep(30)
		print(f"请求失败: {e}")
		return None

def gpt(messages):
	"""使用现有的claude API"""
	# 从配置文件获取API配置
	gpt_config = config['gpt']
	
	# 请求数据
	client = openai.AzureOpenAI(
		azure_endpoint=gpt_config['url'],
		api_version=gpt_config['api_version'],
		api_key=gpt_config['ak'],
	)

	try:
		response = client.chat.completions.create(
			model=gpt_config['model'],
			messages=messages, 
			extra_headers={"X-TT-LOGID": gpt_config['log_id']},  
			#如果改模型需要thinking
			max_tokens=4096,
			extra_body={
				"thinking": {
					"type": "enabled",
					"budget_tokens": 2000,
				}
			}
		)
		import pdb; pdb.set_trace()
		return response.choices[0].message.content
			
	except Exception as e:
		print(f"请求失败: {e}")
		return None
	
def deer_flow(messages):
	"""使用deer-flow API（假设本地运行）"""
	# 从配置文件获取deer-flow配置
	deer_config = config['deer_flow']
	deer_flow_url = deer_config['url']
	
	try:
		# 构建deer-flow的请求格式
		data = {
			"messages": messages,
			"auto_accepted_plan": deer_config['auto_accepted_plan'],
			"max_step_num": deer_config['max_step_num']
		}
		
		headers = {
			"Content-Type": "application/json"
		}
		
		print(f"正在使用deer-flow处理: {messages[0]['content'][:50] if messages and 'content' in messages[0] else 'request'}...")
		response = requests.post(
			url=deer_flow_url,
			headers=headers,
			json=data,
			timeout=deer_config['timeout']
		)
		
		if response.status_code == 200:
			return response.json()
		else:
			print(f"Deer-flow API错误，状态码: {response.status_code}")
			return None
			
	except requests.exceptions.ConnectionError:
		print("错误: 无法连接到deer-flow服务。请确保deer-flow正在localhost:8000运行。")
		print("启动deer-flow命令: cd deer-flow && python server.py")
		return None
	except Exception as e:
		print(f"Deer-flow请求失败: {e}")
		return None
	
@cached
def _get_response(model, messages, nth_generation=0, **kwargs):
	# if messages is str
	if isinstance(messages, str):
		messages = [{"role": "user", "content": messages}]

	try:
		if model == 'gemini_search': 
			response = gemini(messages, search=True)
		elif model == 'gemini':
			response = gemini(messages)
		elif model == 'claude-4-sonnet':
			response = claude(messages)
		elif model.startswith('gpt'):
			response = gpt(messages)
		elif model == 'deer-flow':
			pass
		
		return response

	except Exception as e:
		import traceback 
		logger.error(f'Prompt: {messages[:500]}')
		logger.error(f"Error in _get_response: {str(e)}")

		try:
			if hasattr(response, 'text'):
				logger.error(f"Response: {response.text}")
			else:
				logger.error(f"Response: {response}")
		except Exception as e:
			logger.error(f"Could not print response: {e}")
		
		logger.error(f"Number of input tokens: {num_tokens_from_string(messages[0]['content'])}")

		traceback.print_exc()
		return None

def get_response(post_processing_funcs=[], **kwargs):
	nth_generation = 0

	while True:
		logger.info(f'{nth_generation}th generation')
		response = _get_response(**kwargs, nth_generation=nth_generation)
		logger.info(f'response by LLM: {response}')

		if response is None:
			continue 
		
		# Break if we got a valid response, otherwise retry
		# Run response through post-processing pipeline
		for i, post_processing_func in enumerate(post_processing_funcs):
			if response is None:
				break
			response = post_processing_func(response, **kwargs)

		if response:
			return response
		else:
			nth_generation += 1
			if nth_generation > kwargs.get('max_retry', 5):
				# Return error response with backup data if parse_response failed
				return None

def ensure_question_format(response, **kwargs):
	try:
		assert isinstance(response, dict)
		assert 'entity' in response 
		assert 'questions' in response
		for q in response['questions']:
			assert isinstance(q, dict)
			assert 'entity_type' in q
			
		return response
	except:
		return False


def save_result(filename, result):
	"""保存查询结果到文件"""
	# 创建results目录
	os.makedirs(os.path.dirname(filename), exist_ok=True)
	
	with open(filename, 'w', encoding='utf-8') as f:
		json.dump(result, f, ensure_ascii=False, indent=2)

def format_json_for_display(data, indent_level=0):
	"""
	将JSON数据格式化为易读的文本格式
	"""
	indent = "  " * indent_level
	lines = []
	
	if isinstance(data, dict):
		if not data:
			return "{ }"
		lines.append("{")
		for i, (key, value) in enumerate(data.items()):
			formatted_value = format_json_for_display(value, indent_level + 1)
			comma = "," if i < len(data) - 1 else ""
			lines.append(f"{indent}  \"{key}\": {formatted_value}{comma}")
		lines.append(f"{indent}}}")
		
	elif isinstance(data, list):
		if not data:
			return "[ ]"
		lines.append("[")
		for i, item in enumerate(data):
			formatted_item = format_json_for_display(item, indent_level + 1)
			comma = "," if i < len(data) - 1 else ""
			lines.append(f"{indent}  {formatted_item}{comma}")
		lines.append(f"{indent}]")
		
	elif isinstance(data, str):
		# 处理长字符串，添加换行
		if len(data) > 80:
			return f'"{data[:77]}..."'
		return f'"{data}"'
		
	elif isinstance(data, (int, float)):
		return str(data)
		
	elif isinstance(data, bool):
		return "true" if data else "false"
		
	elif data is None:
		return "null"
		
	else:
		return f'"{str(data)}"'
	
	return "\n".join(lines)

def save_result_txt(filename, results):
	with open(filename, 'w', encoding='utf-8') as f:
		f.write("=" * 80 + "\n\n")
		
		successful_count = 0
		failed_count = 0
		
		for i, (entity, result) in enumerate(results.items(), 1):
			f.write("┌" + "─" * 78 + "┐\n")
			
			if result:
				successful_count += 1
				f.write("│ ✅ 状态: 查询成功\n")
				f.write("├" + "─" * 78 + "┤\n")
				
				# 检查result是否为字典或列表，如果是则美化输出
				if isinstance(result, (dict, list)):
					f.write("│ 📋 数据类型: JSON结构\n")
					f.write("├" + "─" * 78 + "┤\n")
					formatted_json = format_json_for_display(result)
					# 为每行添加边框
					for line in formatted_json.split('\n'):
						f.write(f"│ {line:<76} │\n")
				else:
					# 如果result是字符串但包含JSON，尝试解析并美化
					try:
						parsed_result = json.loads(result)
						f.write("│ 📋 数据类型: 解析后的JSON结构\n")
						f.write("├" + "─" * 78 + "┤\n")
						formatted_json = format_json_for_display(parsed_result)
						for line in formatted_json.split('\n'):
							f.write(f"│ {line:<76} │\n")
					except (json.JSONDecodeError, TypeError):
						# 如果不是JSON格式，直接输出原内容
						f.write("│ 📋 数据类型: 文本内容\n")
						f.write("├" + "─" * 78 + "┤\n")
						# 处理长文本，按行分割
						content_lines = str(result).split('\n')
						for line in content_lines:
							if len(line) > 76:
								# 长行需要换行
								words = line.split(' ')
								current_line = ""
								for word in words:
									if len(current_line + " " + word) <= 76:
										current_line += (" " + word) if current_line else word
									else:
										if current_line:
											f.write(f"│ {current_line:<76} │\n")
										current_line = word
								if current_line:
									f.write(f"│ {current_line:<76} │\n")
							else:
								f.write(f"│ {line:<76} │\n")
			else:
				failed_count += 1
				f.write("│ ❌ 状态: 查询失败\n")
				f.write("├" + "─" * 78 + "┤\n")
				f.write("│ 💬 原因: 无法获取相关信息" + " " * 51 + "│\n")
				
			f.write("└" + "─" * 78 + "┘\n\n")
		
def extract_json(text, **kwargs):
	def _extract_json(text):
		# Use regular expressions to find all content within curly braces
		orig_text = text

		text = re.sub(r'"([^"\\]*(\\.[^"\\]*)*)"', lambda m: m.group().replace('\n', r'\\n'), text) 
		
		#json_objects = re.findall(r'(\{[^{}]*\}|\[[^\[\]]*\])', text, re.DOTALL)

		def parse_json_safely(text):
			try:
				result = json.loads(text)
				return result
			except json.JSONDecodeError:
				results = []
				start = 0
				while start < len(text):
					try:
						obj, end = json.JSONDecoder().raw_decode(text[start:])
						results.append(obj)
						start += end
					except json.JSONDecodeError:
						start += 1
				
				if results:
					longest_json = max(results, key=lambda x: len(json.dumps(x)))
					return longest_json
				else:
					return None
		
		extracted_json = parse_json_safely(text)
		
		if extracted_json:
			return extracted_json
		else:
			logger.error('Error parsing response: ', orig_text)
			return None
	
	res = _extract_json(text)

	return res
