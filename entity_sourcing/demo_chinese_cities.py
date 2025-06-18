#!/usr/bin/env python3
"""
示范：获取50个中文城市实体（更稳定的示范）
"""

from entity_collector import WikidataEntityCollector

def get_chinese_cities():
    """获取50个中文城市实体"""
    print("示范：获取50个中文城市实体")
    print("=" * 50)
    
    # 创建中文收集器
    print("1. 初始化中文实体收集器...")
    collector = WikidataEntityCollector(language='zh', qrank_csv_file='qrank.csv')
    
    # 设置城市类别
    print("2. 设置收集中文城市实体...")
    city_categories = [("中文城市", "Q515")]  # Q515 = 城市
    
    # 收集50个城市实体
    print("3. 开始收集50个中文城市...")
    df = collector.collect_entities(city_categories, limit_per_category=50)
    
    # 显示结果
    print(f"\n✓ 成功收集到 {len(df)} 个中文城市")
    
    if not df.empty:
        print("\n📋 收集到的中文城市列表：")
        print("-" * 80)
        for i, (_, entity) in enumerate(df.iterrows(), 1):
            score = entity['popularity_score']
            popular = "🔥知名" if entity['is_popular'] else "🤫不知名"
            desc = entity.get('description', '')[:30] + '...' if len(entity.get('description', '')) > 30 else entity.get('description', '')
            print(f"{i:2d}. {entity['label']:<20} ({entity['id']}) - {score:>10,.0f} {popular}")
            if desc:
                print(f"    {desc}")
        
        # 统计信息
        print("\n📊 统计信息：")
        total = len(df)
        with_scores = len(df[df['popularity_score'] > 0])
        popular = len(df[df['is_popular'] == True])
        unpopular = total - popular
        
        print(f"   总城市数：{total}")
        print(f"   有知名度评分：{with_scores} ({with_scores/total*100:.1f}%)")
        print(f"   知名城市：{popular}")
        print(f"   不知名城市：{unpopular}")
        
        # 保存结果
        filename = "chinese_cities_50.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\n💾 结果已保存到：{filename}")
        
        # 显示最知名和最不知名的城市
        if with_scores > 0:
            print("\n🏆 最知名的中文城市：")
            top_popular = df[df['popularity_score'] > 0].nlargest(5, 'popularity_score')
            for _, entity in top_popular.iterrows():
                score = entity['popularity_score']
                print(f"   - {entity['label']}: {score:,.0f}")
        
        zero_score = df[df['popularity_score'] == 0]
        if not zero_score.empty:
            print(f"\n🤫 无知名度数据的城市：{len(zero_score)}个")
            print("   示例：")
            for _, entity in zero_score.head(5).iterrows():
                desc = entity.get('description', '')
                print(f"   - {entity['label']} {f'({desc})' if desc else ''}")
                
        # 展示如何筛选不知名城市用于后续研究
        print(f"\n🔬 研究用途：筛选出 {len(zero_score)} 个不知名城市可用于:")
        print("   - 地理知识补全研究")
        print("   - 小众地点推荐系统")
        print("   - 文化多样性分析")
        
    else:
        print("❌ 未能收集到任何城市，可能是网络问题")
    
    print("\n" + "=" * 50)
    print("示范完成！")

if __name__ == "__main__":
    get_chinese_cities()