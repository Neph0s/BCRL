#!/usr/bin/env python3
"""
简单示范：获取不同类型的中文实体（避免复杂UNION查询）
"""

from entity_collector import WikidataEntityCollector
import pandas as pd

def get_simple_mixed_entities():
    """获取混合类型的中文实体（简化版）"""
    print("示范：收集不同类型的中文实体（简化版）")
    print("=" * 60)
    
    # 创建中文收集器
    print("1. 初始化中文实体收集器...")
    collector = WikidataEntityCollector(language='zh', qrank_csv_file='qrank.csv')
    
    # 定义多个小类别，每个类别收集少量实体
    print("2. 设置多个小类别收集...")
    categories = [
        ("中文人物", "Q5"),        # 人物，限制5个
        ("中文城市", "Q515"),      # 城市，限制5个  
        ("中文书籍", "Q571"),      # 书籍，限制5个
        ("中文电影", "Q11424"),    # 电影，限制5个
        ("中文公司", "Q783794"),   # 公司，限制5个
    ]
    
    all_entities = []
    
    # 逐个类别收集，避免大查询
    for i, (category_name, category_id) in enumerate(categories, 1):
        print(f"\n3.{i} 收集{category_name}（Q{category_id}）...")
        
        try:
            df = collector.collect_entities([(category_name, category_id)], limit_per_category=10)
            if not df.empty:
                all_entities.append(df)
                print(f"   ✓ 收集到 {len(df)} 个{category_name}")
            else:
                print(f"   ✗ 未收集到{category_name}")
        except Exception as e:
            print(f"   ✗ 收集{category_name}时出错: {e}")
    
    # 合并所有实体
    if all_entities:
        final_df = pd.concat(all_entities, ignore_index=True)
        
        print(f"\n✓ 总共收集到 {len(final_df)} 个混合类型中文实体")
        
        # 显示结果
        print("\n📋 混合类型中文实体列表：")
        print("-" * 80)
        
        for i, (_, entity) in enumerate(final_df.iterrows(), 1):
            score = entity['popularity_score']
            popular = "🔥知名" if entity['is_popular'] else "🤫不知名"
            category = entity['category']
            desc = entity.get('description', '')
            desc_short = desc[:30] + '...' if len(desc) > 30 else desc
            
            print(f"{i:2d}. {entity['label']:<20} ({entity['id']}) - {score:>10,.0f} {popular} [{category}]")
            if desc_short:
                print(f"    {desc_short}")
        
        # 统计信息
        print("\n📊 统计信息：")
        total = len(final_df)
        with_scores = len(final_df[final_df['popularity_score'] > 0])
        popular = len(final_df[final_df['is_popular'] == True])
        
        print(f"   总实体数：{total}")
        print(f"   有知名度评分：{with_scores} ({with_scores/total*100:.1f}%)")
        print(f"   知名实体：{popular}")
        print(f"   不知名实体：{total - popular}")
        
        # 按类别统计
        print(f"\n🏷️ 按类别统计：")
        category_counts = final_df['category'].value_counts()
        for category, count in category_counts.items():
            cat_df = final_df[final_df['category'] == category]
            avg_score = cat_df['popularity_score'].mean()
            print(f"   {category}: {count}个 (平均知名度: {avg_score:,.0f})")
        
        # 保存结果
        filename = "chinese_simple_mixed_entities.csv"
        final_df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\n💾 结果已保存到：{filename}")
        
        # 显示各类别最知名实体
        print("\n🏆 各类别最知名实体：")
        for category in category_counts.index:
            cat_df = final_df[final_df['category'] == category]
            if len(cat_df) > 0:
                top_entity = cat_df.nlargest(1, 'popularity_score').iloc[0]
                score = top_entity['popularity_score']
                print(f"   {category}: {top_entity['label']} ({score:,.0f})")
        
        print(f"\n🔬 这个混合数据集的用途：")
        print("   ✓ 跨领域中文实体识别训练")
        print("   ✓ 实体链接和消歧任务")
        print("   ✓ 知识图谱补全研究")
        print("   ✓ 多类型实体流行度分析")
        
    else:
        print("❌ 未收集到任何实体")
    
    print("\n" + "=" * 60)
    print("混合类型实体收集完成！")

if __name__ == "__main__":
    get_simple_mixed_entities()