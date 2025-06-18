#!/usr/bin/env python3
"""
使用示范：高效随机采样不指定类别的实体
演示如何获取50个随机中文实体
"""

from entity_collector import WikidataEntityCollector

def demo_random_sampling():
    """示范随机采样功能"""
    print("示范：高效随机采样不指定类别的实体")
    print("=" * 60)
    
    # 创建中文收集器
    print("1. 初始化中文实体收集器...")
    collector = WikidataEntityCollector(language='zh', qrank_csv_file='qrank.csv')
    
    # 设置随机采样
    print("2. 设置随机采样（不指定类别）...")
    sample_size = 50
    
    # 使用None触发随机采样
    print(f"3. 开始随机采样{sample_size}个中文实体...")
    mixed_categories = [("随机采样实体", None)]
    
    df = collector.collect_entities(mixed_categories, limit_per_category=sample_size)
    
    # 显示结果
    print(f"\n✓ 成功采样到 {len(df)} 个随机中文实体")
    
    if not df.empty:
        print("\n📋 随机采样的中文实体列表：")
        print("-" * 90)
        
        for i, (_, entity) in enumerate(df.iterrows(), 1):
            score = entity['popularity_score']
            popular = "🔥知名" if entity['is_popular'] else "🤫不知名"
            sampled_from = entity.get('sampled_from', '未知类型')
            desc = entity.get('description', '')
            desc_short = desc[:40] + '...' if len(desc) > 40 else desc
            
            print(f"{i:2d}. {entity['label']:<25} ({entity['id']}) - {score:>10,.0f} {popular} [{sampled_from}]")
            if desc_short:
                print(f"    {desc_short}")
        
        # 统计信息
        print("\n📊 采样统计：")
        total = len(df)
        with_scores = len(df[df['popularity_score'] > 0])
        popular = len(df[df['is_popular'] == True])
        
        print(f"   总实体数：{total}")
        print(f"   有知名度评分：{with_scores} ({with_scores/total*100:.1f}%)")
        print(f"   知名实体：{popular}")
        print(f"   不知名实体：{total - popular}")
        
        # 按采样来源统计
        if 'sampled_from' in df.columns:
            print(f"\n🎯 按采样来源统计：")
            source_counts = df['sampled_from'].value_counts()
            for source, count in source_counts.items():
                source_df = df[df['sampled_from'] == source]
                avg_score = source_df['popularity_score'].mean()
                print(f"   {source}: {count}个 (平均知名度: {avg_score:,.0f})")
        
        # 知名度分布
        print(f"\n📈 知名度分布：")
        zero_score = len(df[df['popularity_score'] == 0])
        low_score = len(df[(df['popularity_score'] > 0) & (df['popularity_score'] <= 10000)])
        mid_score = len(df[(df['popularity_score'] > 10000) & (df['popularity_score'] <= 100000)])
        high_score = len(df[df['popularity_score'] > 100000])
        
        print(f"   无评分 (=0): {zero_score}个")
        print(f"   低知名度 (1-10K): {low_score}个")
        print(f"   中知名度 (10K-100K): {mid_score}个")
        print(f"   高知名度 (>100K): {high_score}个")
        
        # 保存结果
        filename = f"random_sampled_entities_{sample_size}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\n💾 结果已保存到：{filename}")
        
        # 显示极值
        if with_scores > 0:
            print("\n🏆 最知名实体TOP3：")
            top_entities = df[df['popularity_score'] > 0].nlargest(3, 'popularity_score')
            for _, entity in top_entities.iterrows():
                score = entity['popularity_score']
                source = entity.get('sampled_from', '未知')
                print(f"   - {entity['label']} ({source}): {score:,.0f}")
        
        if zero_score > 0:
            print(f"\n🤫 无知名度数据的实体示例：")
            zero_entities = df[df['popularity_score'] == 0].head(3)
            for _, entity in zero_entities.iterrows():
                source = entity.get('sampled_from', '未知')
                print(f"   - {entity['label']} ({source})")
        
        print(f"\n🔬 随机采样数据集的应用价值：")
        print("   ✓ 跨领域实体表示学习")
        print("   ✓ 知识图谱随机游走算法测试")
        print("   ✓ 实体多样性和覆盖度分析")
        print("   ✓ 长尾实体发现和研究")
        print("   ✓ 无偏采样基准数据集构建")
        
        # 采样质量评估
        sample_types = ["cities", "books", "films", "companies", "fictional characters", 
                       "songs", "universities", "museums", "musical groups", "TV series",
                       "video games", "albums", "artworks", "festivals", "awards"]
        diversity_score = len(source_counts) / len(sample_types) if 'sampled_from' in df.columns else 0
        coverage_score = with_scores / total
        
        print(f"\n📏 采样质量评估：")
        print(f"   类型多样性: {diversity_score:.2f} ({len(source_counts) if 'sampled_from' in df.columns else 0}/{15}个类型)")
        print(f"   数据覆盖度: {coverage_score:.2f} ({with_scores}/{total}有评分)")
        
    else:
        print("❌ 未能采样到任何实体")
    
    print("\n" + "=" * 60)
    print("随机采样示范完成！")

if __name__ == "__main__":
    demo_random_sampling()