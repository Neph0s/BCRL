#!/usr/bin/env python3
"""
示范：获取不指定类别的50个中文实体
"""

from entity_collector import WikidataEntityCollector

def get_chinese_mixed_entities():
    """获取不指定类别的中文实体"""
    print("示范：获取不指定类别的50个中文实体")
    print("=" * 50)
    
    # 创建中文收集器
    print("1. 初始化中文实体收集器...")
    collector = WikidataEntityCollector(language='zh', qrank_csv_file='qrank.csv')
    
    # 设置不指定类别的实体收集
    print("2. 设置混合类型实体收集（不指定P31约束）...")
    mixed_categories = [("混合类型实体", None)]  # None表示不指定类型
    
    # 收集50个实体
    print("3. 开始收集50个中文混合类型实体...")
    df = collector.collect_entities(mixed_categories, limit_per_category=50)
    
    # 显示结果
    print(f"\n✓ 成功收集到 {len(df)} 个中文实体")
    
    if not df.empty:
        print("\n📋 收集到的中文实体列表：")
        print("-" * 70)
        for i, (_, entity) in enumerate(df.iterrows(), 1):
            score = entity['popularity_score']
            popular = "🔥知名" if entity['is_popular'] else "🤫不知名"
            print(f"{i:2d}. {entity['label']} ({entity['id']}) - {score:,.0f} {popular}")
        
        # 统计信息
        print("\n📊 统计信息：")
        total = len(df)
        with_scores = len(df[df['popularity_score'] > 0])
        popular = len(df[df['is_popular'] == True])
        unpopular = total - popular
        
        print(f"   总实体数：{total}")
        print(f"   有知名度评分：{with_scores} ({with_scores/total*100:.1f}%)")
        print(f"   知名实体：{popular}")
        print(f"   不知名实体：{unpopular}")
        
        # 保存结果
        filename = "chinese_mixed_entities_50.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\n💾 结果已保存到：{filename}")
        
        # 显示最知名和最不知名的实体
        if with_scores > 0:
            print("\n🏆 最知名的实体：")
            top_popular = df[df['popularity_score'] > 0].nlargest(3, 'popularity_score')
            for _, entity in top_popular.iterrows():
                score = entity['popularity_score']
                print(f"   - {entity['label']}: {score:,.0f}")
        
        zero_score = df[df['popularity_score'] == 0]
        if not zero_score.empty:
            print(f"\n🤫 无知名度数据的实体：{len(zero_score)}个")
            print("   示例：")
            for _, entity in zero_score.head(3).iterrows():
                print(f"   - {entity['label']}")
    else:
        print("❌ 未能收集到任何实体，可能是网络问题或查询超时")
    
    print("\n" + "=" * 50)
    print("示范完成！")

if __name__ == "__main__":
    get_chinese_mixed_entities()