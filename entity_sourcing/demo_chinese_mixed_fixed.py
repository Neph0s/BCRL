#!/usr/bin/env python3
"""
示范：获取不指定类别的50个中文实体（修复版）
"""

from entity_collector import WikidataEntityCollector

def get_chinese_mixed_entities_fixed():
    """获取混合类型的中文实体（修复版）"""
    print("示范：获取混合类型的50个中文实体（优化版）")
    print("=" * 60)
    
    # 创建中文收集器
    print("1. 初始化中文实体收集器...")
    collector = WikidataEntityCollector(language='zh', qrank_csv_file='qrank.csv')
    
    # 设置混合类型实体收集（优化查询避免超时）
    print("2. 设置混合类型实体收集（从多个常见类型采样）...")
    mixed_categories = [("混合类型实体", None)]  # None会触发优化的UNION查询
    
    # 收集50个实体
    print("3. 开始收集50个中文混合类型实体...")
    print("   注意：这会从人物、城市、书籍、电影、公司等类型中采样")
    df = collector.collect_entities(mixed_categories, limit_per_category=50)
    
    # 显示结果
    print(f"\n✓ 成功收集到 {len(df)} 个中文混合类型实体")
    
    if not df.empty:
        # 按类型分组显示
        print("\n📋 按实体类型分组显示：")
        print("-" * 80)
        
        # 根据QID前缀或知名度猜测实体类型
        def guess_entity_type(entity):
            desc = entity.get('description', '').lower()
            if '城市' in desc or '市' in desc:
                return '🏙️ 城市'
            elif '公司' in desc or '企业' in desc:
                return '🏢 公司'
            elif '电影' in desc or '影片' in desc:
                return '🎬 电影'
            elif '书' in desc or '小说' in desc:
                return '📚 书籍'
            elif '歌曲' in desc or '音乐' in desc:
                return '🎵 音乐'
            elif '人' in desc or '作家' in desc or '演员' in desc:
                return '👤 人物'
            elif '大学' in desc or '学院' in desc:
                return '🎓 大学'
            else:
                return '❓ 其他'
        
        # 为实体添加类型标签
        df_with_type = df.copy()
        df_with_type['guessed_type'] = df_with_type.apply(guess_entity_type, axis=1)
        
        # 按猜测类型分组显示
        for entity_type in df_with_type['guessed_type'].unique():
            type_entities = df_with_type[df_with_type['guessed_type'] == entity_type]
            print(f"\n{entity_type} ({len(type_entities)}个):")
            
            for i, (_, entity) in enumerate(type_entities.head(10).iterrows(), 1):
                score = entity['popularity_score']
                popular = "🔥知名" if entity['is_popular'] else "🤫不知名"
                desc = entity.get('description', '')
                desc_short = desc[:40] + '...' if len(desc) > 40 else desc
                print(f"  {i}. {entity['label']:<25} ({entity['id']}) - {score:>8,.0f} {popular}")
                if desc_short:
                    print(f"     {desc_short}")
        
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
        
        # 类型分布
        print(f"\n🏷️ 类型分布：")
        type_counts = df_with_type['guessed_type'].value_counts()
        for entity_type, count in type_counts.items():
            print(f"   {entity_type}: {count}个")
        
        # 保存结果
        filename = "chinese_mixed_entities_fixed_50.csv"
        df_with_type.to_csv(filename, index=False, encoding='utf-8')
        print(f"\n💾 结果已保存到：{filename}")
        
        # 显示各类型中最知名的实体
        print("\n🏆 各类型最知名实体：")
        for entity_type in type_counts.head(5).index:
            type_entities = df_with_type[df_with_type['guessed_type'] == entity_type]
            if len(type_entities) > 0:
                top_entity = type_entities.nlargest(1, 'popularity_score').iloc[0]
                score = top_entity['popularity_score']
                print(f"   {entity_type}: {top_entity['label']} ({score:,.0f})")
                
        print(f"\n🔬 研究价值：")
        print("   ✓ 跨领域实体样本，适合多领域知识图谱研究")
        print("   ✓ 包含知名度梯度，可研究实体流行度分布")
        print("   ✓ 多语言标签，适合中文NLP任务")
        print("   ✓ 不知名实体可用于长尾知识发现")
        
    else:
        print("❌ 未能收集到任何实体，可能是网络问题")
    
    print("\n" + "=" * 60)
    print("混合类型实体收集示范完成！")

if __name__ == "__main__":
    get_chinese_mixed_entities_fixed()