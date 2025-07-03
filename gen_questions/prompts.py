# Language configuration
PROMPTS = {
    'zh': {
        'search_prompt': """请帮我搜集关于'{entity}'的所有相关知识，整合成一份详细的百科式的资料。使用markdown格式。务必真实、综合、全面，不遗漏重要信息。你要甄别信息的真实性、信息源的可信度，不要包含或编造虚假的信息。""",
        
        'search_second_prompt': """请继续搜索，对你已经搜集到的知识进行验证和扩充，比如搜索密切相关的人物、事件、物品等实体。""",
        
        'question_generate_prompt': """请根据{entity}的相关知识，组合{N_I_LOW}到{N_I_HIGH}条信息，设计{N_Q}道以{entity}为答案的中文问题。

这些题目应该满足：
1）表述自然：表述流畅自然；
2）通用表达：使用现实生活中的通俗词汇。不出现特殊的概念（比如小说或游戏中的概念）和具体的名字（包括人名、物品名等），可以用通俗的词汇代替（如“斗气”->“超能力”）；
3）问题明确：问题明确、客观，不包含主观评价、判断和推理；
4）多样性：不同题目的表达方式具有多样性；描述的信息可以有重合，但不完全相同。
5）搜索困难：每道题目应具有一定挑战性，需要在互联网上多次使用搜索引擎才能回答，但单次搜索无法直接得到答案；
6）限定范围：每道问题应限定出题的范围（答案的类型），避免答题者往其他方向思考。你需要提供一个从宽泛到具体的实体类型列表（2~3个），比如["小说角色", "奇幻小说角色", "《冰与火之歌》中的角色"]。你需要在问题中用占位符{entity_type}表示实体类型。

题目中描述的信息应该满足：
1）搜索线索：提供适量用于搜索的关键词，但避免单次搜索直接得到答案；
2）关键特征：包含1-2条该答案的关键特征，想到答案就容易联想到这些特征（信息），P(信息|答案)高；
3）避免直接关联：避免过于独特或高度特定、能直接联想或搜索到答案的信息，P(答案｜信息)不高；每条信息应适用于多个潜在答案；对部分信息进行模糊处理（比如“10月13日” -> “10月”）；
4）客观可验证：所有信息均为客观事实，可验证，避免主观评价或推测；
5）答案唯一：多条信息组合后能够确保答案唯一，即使在最宽泛的{entity_type}范围内也不存在其他可能答案；
6）排除其他实体：包含1-2条用于排除其他候选实体的信息（如时间、地点等），这些信息不提供线索，可以经过模糊化处理，不直接关联到目标实体。

你需要确认每道题目是否满足以上要求。请输出JSON格式，如：{
    "entity": <entity name>, 
    "questions": [
        {
            "entity_type": <list of entity_types, from broad to specific>, 
            "thinking": <string. share your reasoning behind the question design and evaluate whether it meets the specified requirements. If not, iterate on your thinking and create a new design. in Chinese.>,
            "question": <question description, include a placeholder {entity_type} for entity type. use question (e.g. which ...?) or command (e.g. please find this ...) sentence>, 
        },
        ...
    ]
}

以下是一些题目的例子，供你参考：
### 示例1
1990年至1994年间，哪一场{entity_type}由巴西裁判执法，有四张黄牌（每队两张），其中三张不是在上半场出示的，比赛中有四次换人，其中一次是因为在比赛前25分钟内的伤病。说出这场比赛的两支队伍。
（实体类型：足球比赛；答案：爱尔兰对罗马尼亚）

### 示例2
请找出这个{entity_type}：他偶尔会打破第四面墙与观众互动，他的背景故事涉及无私修行者的帮助，以幽默著称，他的电视节目在1960年至1980年间播出，总集数少于50集。
（实体类型：虚构角色；答案：塑胶人）

### 示例3
请找出这篇{entity_type}：发表于2023年6月前，提到文化传统、科学过程和烹饪创新。由三位作者共同撰写：其中一位是西孟加拉邦的助理教授，另一位拥有博士学位。
（实体类型：研究出版物；答案：《面包制作基础：面包科学》）

### 示例4
请找出这位{entity_type}：她曾被当作交易的一部分，嫁给了一位异族首领。后来，她凭借自身努力赢得了草原民族的效忠，并带领他们跨越海洋。
（实体类型：虚构角色；答案：丹妮莉丝·坦格利安（《权力的游戏》）
"""
    },
    'en': {
        'search_prompt': """Please help me collect all relevant knowledge about '{entity}' and integrate it into a detailed encyclopedic document. Use markdown format. Be truthful, comprehensive, and thorough, without omitting important information. You must verify the authenticity of information and the credibility of sources. Do not include or fabricate false information.""",
        
        'search_second_prompt': """Please continue searching to verify and expand on the knowledge you have already collected, such as searching for closely related people, events, objects, and other entities.""",
        
        'question_generate_prompt': """Based on the relevant knowledge about {entity}, combine {N_I_LOW} to {N_I_HIGH} pieces of information to design {N_Q} English questions with {entity} as the answer.

These questions should satisfy:
1) Natural expression: Fluent and natural phrasing;
2) Common language: Use everyday vocabulary from real life. Avoid special concepts (such as concepts from novels or games) and specific names (including personal names, item names, etc.), which can be replaced with common terms (e.g., "The force" -> "superpower");
3) Clear questions: Questions should be clear, objective, and not contain subjective evaluations, judgments, or reasoning;
4) Diversity: Different questions should have diverse expressions; the described information can overlap but should not be completely identical.
5) Search difficulty: Each question should have a certain level of challenge, requiring multiple internet searches to answer, but single searches cannot directly yield the answer;
6) Scope limitation: Each question should limit the scope of the question (type of answer) to prevent respondents from thinking in other directions. You need to provide a list of types from broad to specific (2~3), such as ["novel character", "fantasy novel character", "character from 'A Song of Ice and Fire'"]. You need to use the placeholder {entity_type} in the question to represent the entity type.

The information described in the questions should satisfy:
1) Search clues: Provide appropriate keywords for searching, but avoid single searches that directly yield the answer;
2) Key characteristics: Include 1-2 key characteristics of the answer, features that are easily associated when thinking of the answer, with high P(information|answer);
3) Avoid direct association: Avoid overly unique or highly specific information that can directly lead to or search for the answer, P(answer|information) should not be high; each piece of information should apply to multiple potential answers; apply vagueness to some information (e.g., "October 13" -> "October");
4) Objective and verifiable: All information should be objective facts and verifiable, avoiding subjective evaluations or speculation;
5) Unique answer: Multiple pieces of information combined should ensure a unique answer, even within the broadest {entity_type} scope, with no other possible answers;
6) Exclude other entities: Include 1-2 pieces of information for excluding other candidate entities (such as time, location, etc.), this information does not provide clues, can be processed with vagueness, and does not directly associate with the target entity.

You need to confirm whether each question satisfies the above requirements. Please output in JSON format, as follows: {
    "entity": <entity name>, 
    "questions": [
        {
            "entity_type": <list of entity_types, from broad to specific>, 
            "thinking": <string. share your reasoning behind the question design and evaluate whether it meets the specified requirements. If not, iterate on your thinking and create a new design. in English.>,
            "question": <question description, include a placeholder {entity_type} for entity type. use question (e.g. which ...?) or command (e.g. please find this ...) sentence>, 
        },
        ...
    ]
}

Here are some examples for your reference:
### Example 1
Between 1990 and 1994, which {entity_type} was officiated by a Brazilian referee, had four yellow cards (two for each team), of which three were not shown in the first half, had four substitutions during the match, one of which was due to an injury within the first 25 minutes. Name the two teams in this match.
(Entity type: football match; Answer: Ireland vs Romania)

### Example 2
Find this {entity_type}: He occasionally breaks the fourth wall to interact with the audience, his backstory involves help from selfless practitioners, he is known for humor, his TV show aired between 1960 and 1980, with fewer than 50 total episodes.
(Entity type: fictional character; Answer: Plastic Man)

### Example 3
Find this {entity_type}: Published before June 2023, mentions cultural traditions, scientific processes, and culinary innovation. Co-authored by three writers: one is an assistant professor from West Bengal, another has a doctoral degree.
(Entity type: research publication; Answer: "Fundamentals of Bread Making: The Science of Bread")

### Example 4
Find this {entity_type}: She was once used as part of a trade deal, married to a foreign tribal leader. Later, through her own efforts, she won the loyalty of nomadic peoples and led them across the ocean.
(Entity type: fictional character; Answer: Daenerys Targaryen (Game of Thrones))
"""
    }
}

def get_prompt(prompt_name, language='zh'):
    """
    Get a prompt in the specified language.
    
    Args:
        prompt_name (str): Name of the prompt ('search_prompt', 'search_second_prompt', 'question_generate_prompt')
        language (str): Language code ('zh' for Chinese, 'en' for English)
    
    Returns:
        str: The prompt in the specified language
    """
    if language not in PROMPTS:
        raise ValueError(f"Unsupported language: {language}. Supported languages: {list(PROMPTS.keys())}")
    
    if prompt_name not in PROMPTS[language]:
        raise ValueError(f"Prompt '{prompt_name}' not found for language '{language}'")
    
    return PROMPTS[language][prompt_name]

# Backward compatibility - keep the original variables for 'zh' (Chinese)
search_prompt = PROMPTS['zh']['search_prompt']
search_second_prompt = PROMPTS['zh']['search_second_prompt'] 
question_generate_prompt = PROMPTS['zh']['question_generate_prompt']