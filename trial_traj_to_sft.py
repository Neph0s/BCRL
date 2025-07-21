#!/usr/bin/env python3
import pandas as pd
import json
import argparse
import sys


def convert_parquet_to_oai(file_path):
    """
    读取parquet文件并随机采样数据，以格式化JSON输出
    
    Args:
        file_path: parquet文件路径
        num_samples: 采样数量，默认2条
    """

    # 读取parquet文件
    df = pd.read_parquet(file_path)
    
    print(f'Dataset Size: {len(df)}')
    # 随机采样
    json_data = []
    sft_data = []
    for _, row in df.iterrows():
        row_dict = {}
        for col, value in row.items():
            # 处理numpy数组和其他不可序列化的类型
            if hasattr(value, 'tolist'):
                row_dict[col] = value.tolist()
            elif pd.isna(value):
                row_dict[col] = None
            else:
                row_dict[col] = value
        
        if row_dict["score"] == 1 and row_dict["len"] < 32000:
            messages_str = row_dict['prompt'] + row_dict['gen']
            orig_messages_str = messages_str
            import re
            messages_str = re.sub(r'(\[发布时间\] \d{4})年(\d{1,2})月(\d{1,2})日', r'\1-\2-\3', messages_str)
            messages_str = messages_str.replace("[发布时间] 无", "[publish_time] None")
            for zh_word, en_word in [('[摘要]', '[summary]'), ('[标题]', '[title]'), ('[序号]', '[number]'), ('[发布时间]', '[publish_time]'), ('[来源]', '[source]'), ('（星期一）', '(Monday)'), ('（星期二）', '(Tuesday)'), ('（星期三）', '(Wednesday)'), ('（星期四）', '(Thursday)'), ('（星期五）', '(Friday)'), ('（星期六）', '(Saturday)'), ('（星期日）', '(Sunday)'),]:
                messages_str = messages_str.replace(zh_word, en_word)
            # 用正则表达式 识别这样的日期 2018年5月7日 变成 2018-05-07
            
            messages = []
            message_pieces = messages_str.split('<[BOS_never_used_51bce0c785ca2f68081bfa7d91973934]>')[1:]
            last_tool = None
            for message_piece in message_pieces:
                role = None
                for m_role in ['system', 'user', 'assistant', 'tool']:
                    if message_piece.startswith(m_role):
                        role = m_role 
                        break
                assert role is not None

                message_piece = message_piece.replace(role, '', 1).strip(' \n')
                try:
                    assert ('<[EOS_never_used_51bce0c785ca2f68081bfa7d91973934]>' in message_piece)
                except:
                    import pdb; pdb.set_trace()
                message_piece = message_piece.split('<[EOS_never_used_51bce0c785ca2f68081bfa7d91973934]>')[0]
                message_piece = message_piece.strip()
                
                assert (len(message_piece) > 0)
                if '|FunctionCallBegin|' in message_piece:
                    function_call = message_piece.split('|FunctionCallBegin|')[-1]
                    if '"read_beautiful_soup"' in function_call:
                        last_tool = '"read_beautiful_soup"'
                    elif '"search_bing"' in function_call:
                        last_tool = '"search_bing"'
                    else:
                        if not ('"function_name"' in function_call):
                            import pdb; pdb.set_trace()
                    
                if len(messages) > 0 and messages[-1]['role'] == role:
                    messages[-1]['content'] += message_piece
                else:
                    if role == 'assistant': 
                        loss_mask = 1.0
                    else:
                        loss_mask = 0.0
                    if role == 'tool':
                        n = last_tool
                        assert n is not None
                    else:
                        n = ''
                    messages.append({"role": role, "content": message_piece, "loss_mask": loss_mask, "name": n})
                    
            sft_data.append({'messages': messages})
                    
        json_data.append(row_dict)
    
    sft_data = sft_data[:4000]

    print(f'RS 样本数量 {len(sft_data)}')
    output_file = file_path.replace('val.', '', 1).replace('.parquet', '.json')
    with open(output_file, 'w') as f:
        json.dump(sft_data, f, indent=2, ensure_ascii=False)
    print(f"SFT JSON文件已保存到: {output_file}")

    import pdb; pdb.set_trace()
    output_file = file_path.replace('val.', '', 1)
    df = pd.DataFrame(sft_data)
    df.to_parquet(output_file)
    print(f"SFT Parquet文件已保存到: {output_file}")
    


    return
        


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="可视化parquet文件数据")
    parser.add_argument("dir_path", help="parquet文件路径")

    args = parser.parse_args()
    # 遍历所有./traj下的parquet文件，如果不存在同名json文件，则调用convert_parquet_to_json
    import glob
    import os

    #for file_path in glob.glob(f'{args.dir_path}/**/val*.parquet', recursive=True):
    for file_path in glob.glob(f'{args.dir_path}/**/val*.parquet', recursive=True):
        #if not os.path.exists(file_path.replace('val.', '')):
        convert_parquet_to_oai(file_path)
    
# python visualize_parquet.py /root/wxt/bc/gen_questions/parquet_files_v1/train4.parquet
# python visualize_parquet.py /root/wxt/DeepResearcher/data/test_small.parquet
# python visualize_parquet.py /root/wxt/DeepResearcher/data/browsecomp-mini.parquet
# python visualize_parquet.py hdfs://haruna/home/byte_data_seed/hdd_ygdt/user/wangxintao.agi/bcrl/data/parquet_files_v2/bc-syn-train-v2.parquet