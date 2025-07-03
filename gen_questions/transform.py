import json
import random

random.seed(42)

zh_file = "./results/bc_questions_0628_zh_v2json"
en_file = "./results/bc_questions_0629_en_v2.json"


specified_test_ids = ['阿蒙（诡秘之主）[WIKI:nan]', '土伯（牧神记）[WIKI:nan]', '丹妮莉丝·坦格利安（冰与火之歌）[WIKI:nan]', '芙宁娜（原神）[WIKI:nan]', '雪王（蜜雪冰城的吉祥物）[WIKI:nan]', '上海市进才中学[WIKI:nan]', '鹿乃（Kano）[WIKI:nan]', '哈姆雷特[WIKI:nan]', '盖欧卡[WIKI:nan]', 'Sam Altman[WIKI:nan]', 'Ryner Lute[WIKI:nan]', 'Airi Tazume[WIKI:nan]', 'Amon (Lord of Mysteries)[WIKI:nan]', 'Tu Bo (Tales of Qin Mu)[WIKI:nan]', 'Daenerys Targaryen[WIKI:nan]', 'Furina (Genshin Impact)[WIKI:nan]', 'Snow King (Mixue Bingcheng)[WIKI:nan]', 'Jincai High School Shanghai[WIKI:nan]', 'Kano (Kanoshirayuki)[WIKI:nan]', 'Hamlet[WIKI:nan]', 'Kyogre[WIKI:nan]', 'Sam Altman[WIKI:nan]', 'Ryner Lute[WIKI:nan]', 'Airi Tazume[WIKI:nan]']

num_entities = {'zh': 5000, 'en': 10000}
input_files = {'zh': zh_file, 'en': en_file}

train_data = []
test_data = []

curriculum = False

for lang in ['en', 'zh']:
    with open(input_files[lang], 'r') as f:
        data = json.load(f)
    
    print(f'Initially {len(data)} entities')
    data_ = {}
    for k, v in data.items():
        try:
            assert(v['search_response'] is not None and v['search_again_response'] is not None and v['question_response'] is not None and len(v['question_response']) == 2)
            for qr in v['question_response']:
                for qs in qr['questions']:
                    assert(len(qs['entity_type']) >= 2)
                    assert('{entity_type}' in qs['question'])
            data_[k] = v
        except:
            continue
        if len(data_) >= num_entities[lang]:
            break
    data = data_

    print(f'After removing invalid: {len(data)} entities')


    entity_ids = [id for id in data.keys()]
    print(f"lang: {lang}, entity_ids: {len(entity_ids)}")

    entity_ids_test = [eid for eid in entity_ids if eid in specified_test_ids]
    remaining_entity_ids = [eid for eid in entity_ids if eid not in specified_test_ids]
    random.shuffle(remaining_entity_ids)

    n_train = int(len(entity_ids) * 0.9)
    n_test = int(len(entity_ids) * 0.1)
    split_idx = n_test-len(entity_ids_test)
    entity_ids_test += remaining_entity_ids[:split_idx]
    entity_ids_train = remaining_entity_ids[split_idx:][:n_train]

    print(f"test: {len(entity_ids_test)} {entity_ids_test[:15]}, train: {len(entity_ids_train)}")

    if lang == 'zh':
        data_source = "bc-syn-zh"
    else:
        data_source = "bc-syn"

    for eid in entity_ids_train:
        if curriculum:
            pass
        else:
            entity_data = data[eid]
            entity_name = entity_data['entity']
            i_q = 0
            for qr in entity_data['question_response']:
                for qs in qr['questions']:
                    # 统一使用最大的范围
                    question = qs['question'].replace('{entity_type}', qs['entity_type'][0])
                    sample = {
                        "data_source": data_source,
                        "prompt": [{"content": question, "role": "user"}],
                        "ability": "search & question answering",
                        "reward_model": {
                            "ground_truth": entity_name,
                            "style": "unknown"
                        },
                        "extra_info": {
                            "entity_name": entity_name,
                            "index": f"{data_source}-{eid}-{i_q}",
                            "question": question, 
                            "entity_info": entity_data["entity_info"],
                            "split": "train"
                        }
                    }
                    i_q += 1 
                    train_data.append(sample)
    
    for eid in entity_ids_test:
        if curriculum:
            pass
        else:
            entity_data = data[eid]
            entity_name = entity_data['entity']
            
            
            qr = entity_data['question_response'][0]
            qs = qr['questions'][0]
            question = qs['question'].replace('{entity_type}', qs['entity_type'][0])
            sample = {
                "data_source": data_source,
                "prompt": [{"content": question, "role": "user"}],
                "ability": "search & question answering",
                "reward_model": {
                    "ground_truth": entity_name,
                    "style": "unknown"
                },
                "extra_info": {
                    "entity_name": entity_name,
                    "index": f"{data_source}-{eid}-0",
                    "question": question, 
                    "entity_info": entity_data["entity_info"],
                    "split": "test"
                }
            }
            test_data.append(sample)
    
print(f"train: {len(train_data)}, test: {len(test_data)}")

output_path_train = './results/bc-syn-train-v2.parquet'
output_path_test = './results/bc-syn-test-v2.parquet'

import pandas as pd

pd.DataFrame(train_data).to_parquet(output_path_train)
pd.DataFrame(test_data).to_parquet(output_path_test)
        
   
