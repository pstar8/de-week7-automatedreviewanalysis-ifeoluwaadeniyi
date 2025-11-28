import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os


def calculate_sentiment_breakdown(df, class_column='Class Name'):
    try:
        if 'AI Sentiment' not in df.columns:
            print("Error: 'AI Sentiment' column not found!")
            return None
        
        if class_column not in df.columns:
            print(f"Error: '{class_column}' column not found!")
            print(f"   Available columns: {', '.join(df.columns)}")
            return None
        
        breakdown = df.groupby([class_column, 'AI Sentiment']).size().unstack(fill_value=0)
        breakdown_pct = breakdown.div(breakdown.sum(axis=1), axis=0) * 100
        breakdown['Total'] = breakdown.sum(axis=1)
        
        return breakdown, breakdown_pct
    
    except Exception as e:
        print(f"Error calculating sentiment breakdown: {e}")
        return None, None


def identify_top_classes(df, breakdown_pct, class_column='Class Name'):
    try:
        results = {}
        
        # Highest Positive Sentiment
        if 'Positive' in breakdown_pct.columns:
            top_positive = breakdown_pct['Positive'].idxmax()
            top_positive_pct = breakdown_pct['Positive'].max()
            results['highest_positive'] = {
                'class': top_positive,
                'percentage': top_positive_pct
            }
            print(f"\n✅ Highest Positive Sentiment:")
            print(f"   Class: {top_positive}")
            print(f"   Positive %: {top_positive_pct:.2f}%")
        
        # Highest Negative Sentiment
        if 'Negative' in breakdown_pct.columns:
            top_negative = breakdown_pct['Negative'].idxmax()
            top_negative_pct = breakdown_pct['Negative'].max()
            results['highest_negative'] = {
                'class': top_negative,
                'percentage': top_negative_pct
            }
            print(f"\n⚠️  Highest Negative Sentiment:")
            print(f"   Class: {top_negative}")
            print(f"   Negative %: {top_negative_pct:.2f}%")
        
        # Highest Neutral Sentiment
        if 'Neutral' in breakdown_pct.columns:
            top_neutral = breakdown_pct['Neutral'].idxmax()
            top_neutral_pct = breakdown_pct['Neutral'].max()
            results['highest_neutral'] = {
                'class': top_neutral,
                'percentage': top_neutral_pct
            }
            print(f"\n➖ Highest Neutral Sentiment:")
            print(f"   Class: {top_neutral}")
            print(f"   Neutral %: {top_neutral_pct:.2f}%")
        
        return results
    
    except Exception as e:
        print(f"Error identifying top classes: {e}")
        return {}


def create_visualizations(df, breakdown, breakdown_pct, class_column='Class Name'):
    try:
        os.makedirs('charts', exist_ok=True)
        
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (12, 6)
        
        saved_charts = []
        
        print("\n Creating Chart 1: Overall Sentiment Distribution...")
        fig, ax = plt.subplots(figsize=(8, 8))
        
        sentiment_counts = df['AI Sentiment'].value_counts()
        colors = {'Positive': '#4CAF50', 'Negative': '#F44336', 'Neutral': '#FFC107'}
        chart_colors = [colors.get(sent, '#999999') for sent in sentiment_counts.index]
        
        ax.pie(sentiment_counts.values, 
               labels=sentiment_counts.index, 
               autopct='%1.1f%%',
               startangle=90,
               colors=chart_colors,
               textprops={'fontsize': 12, 'weight': 'bold'})
        ax.set_title('Overall Sentiment Distribution', fontsize=16, weight='bold', pad=20)
        
        chart1_path = 'charts/overall_sentiment_pie.png'
        plt.tight_layout()
        plt.savefig(chart1_path, dpi=300, bbox_inches='tight')
        plt.close()
        saved_charts.append(chart1_path)
        print(f"   ✅ Saved: {chart1_path}")
        
        print("\n Creating Chart 2: Sentiment by Clothing Class...")
        fig, ax = plt.subplots(figsize=(14, 8))
        
        breakdown_pct_plot = breakdown_pct[['Positive', 'Negative', 'Neutral']].copy()
        breakdown_pct_plot.plot(kind='bar', 
                                stacked=True, 
                                ax=ax,
                                color=['#4CAF50', '#F44336', '#FFC107'],
                                width=0.7)
        
        ax.set_title('Sentiment Distribution by Clothing Class (%)', fontsize=16, weight='bold', pad=20)
        ax.set_xlabel('Clothing Class', fontsize=12, weight='bold')
        ax.set_ylabel('Percentage (%)', fontsize=12, weight='bold')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
        ax.legend(title='Sentiment', title_fontsize=12, fontsize=10)
        ax.grid(axis='y', alpha=0.3)
        
        chart2_path = 'charts/sentiment_by_class_stacked.png'
        plt.tight_layout()
        plt.savefig(chart2_path, dpi=300, bbox_inches='tight')
        plt.close()
        saved_charts.append(chart2_path)
        print(f" Saved: {chart2_path}")
        
        print("\n Creating Chart 3: Sentiment Counts by Class...")
        fig, ax = plt.subplots(figsize=(14, 8))
        
        breakdown_plot = breakdown[['Positive', 'Negative', 'Neutral']].copy()
        
        breakdown_plot.plot(kind='bar', 
                           ax=ax,
                           color=['#4CAF50', '#F44336', '#FFC107'],
                           width=0.7)
        
        ax.set_title('Sentiment Counts by Clothing Class', fontsize=16, weight='bold', pad=20)
        ax.set_xlabel('Clothing Class', fontsize=12, weight='bold')
        ax.set_ylabel('Number of Reviews', fontsize=12, weight='bold')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
        ax.legend(title='Sentiment', title_fontsize=12, fontsize=10)
        ax.grid(axis='y', alpha=0.3)
        
        chart3_path = 'charts/sentiment_by_class_grouped.png'
        plt.tight_layout()
        plt.savefig(chart3_path, dpi=300, bbox_inches='tight')
        plt.close()
        saved_charts.append(chart3_path)
        print(f"   ✅ Saved: {chart3_path}")

        print("\n Creating Chart 4: Top Performing Classes...")
        fig, axes = plt.subplots(1, 3, figsize=(16, 5))
        
        sentiments = ['Positive', 'Negative', 'Neutral']
        colors_map = {'Positive': '#4CAF50', 'Negative': '#F44336', 'Neutral': '#FFC107'}
        
        for idx, sentiment in enumerate(sentiments):
            if sentiment in breakdown_pct.columns:
                top_5 = breakdown_pct[sentiment].nlargest(5).sort_values(ascending=True)
                
                axes[idx].barh(range(len(top_5)), top_5.values, color=colors_map[sentiment])
                axes[idx].set_yticks(range(len(top_5)))
                axes[idx].set_yticklabels(top_5.index)
                axes[idx].set_xlabel('Percentage (%)', fontsize=10, weight='bold')
                axes[idx].set_title(f'Top 5 Classes - {sentiment}', fontsize=12, weight='bold')
                axes[idx].grid(axis='x', alpha=0.3)
                
                for i, v in enumerate(top_5.values):
                    axes[idx].text(v + 1, i, f'{v:.1f}%', va='center', fontsize=9)
        
        chart4_path = 'charts/top_classes_comparison.png'
        plt.tight_layout()
        plt.savefig(chart4_path, dpi=300, bbox_inches='tight')
        plt.close()
        saved_charts.append(chart4_path)
        print(f"   ✅ Saved: {chart4_path}")
        
        print(f"\n All visualizations created successfully!")
        print(f"   Total charts: {len(saved_charts)}")
        print(f"   Location: ./charts/")
        
        return saved_charts
    
    except Exception as e:
        print(f"Error creating visualizations: {e}")
        return []


def generate_insights_report(df, breakdown, breakdown_pct, top_classes, class_column='Class Name'):    
    report = []
    report.append("=" * 70)
    report.append("AUTOMATED REVIEW ANALYSIS - INSIGHTS REPORT")
    report.append("=" * 70)
    
    # Overall Statistics
    total_reviews = len(df)
    sentiment_counts = df['AI Sentiment'].value_counts()
    
    report.append("\n📊 OVERALL STATISTICS")
    report.append("-" * 70)
    report.append(f"Total Reviews Analyzed: {total_reviews}")
    report.append(f"\nSentiment Distribution:")
    for sentiment, count in sentiment_counts.items():
        pct = (count / total_reviews) * 100
        report.append(f"  • {sentiment}: {count} ({pct:.1f}%)")
    
    # Action Items
    action_needed = df[df['Action Needed?'] == 'Yes']
    report.append(f"\n⚠️  Reviews Requiring Action: {len(action_needed)} ({len(action_needed)/total_reviews*100:.1f}%)")
    
    # Top Performing Classes
    report.append("\n🏆 TOP PERFORMING CLASSES")
    report.append("-" * 70)
    
    if 'highest_positive' in top_classes:
        report.append(f"\n✅ Highest Positive Sentiment:")
        report.append(f"  Class: {top_classes['highest_positive']['class']}")
        report.append(f"  Positive Reviews: {top_classes['highest_positive']['percentage']:.2f}%")
    
    if 'highest_negative' in top_classes:
        report.append(f"\nHighest Negative Sentiment:")
        report.append(f"  Class: {top_classes['highest_negative']['class']}")
        report.append(f"  Negative Reviews: {top_classes['highest_negative']['percentage']:.2f}%")
    
    if 'highest_neutral' in top_classes:
        report.append(f"\n➖ Highest Neutral Sentiment:")
        report.append(f"  Class: {top_classes['highest_neutral']['class']}")
        report.append(f"  Neutral Reviews: {top_classes['highest_neutral']['percentage']:.2f}%")
    
    # Key Insights
    report.append("\n💡 KEY INSIGHTS")

    positive_pct = (sentiment_counts.get('Positive', 0) / total_reviews) * 100
    negative_pct = (sentiment_counts.get('Negative', 0) / total_reviews) * 100
    
    if positive_pct > 60:
        report.append("✓ Overall customer satisfaction is HIGH (>60% positive reviews)")
    elif positive_pct > 40:
        report.append("~ Customer satisfaction is MODERATE (40-60% positive reviews)")
    else:
        report.append("✗ Customer satisfaction needs IMPROVEMENT (<40% positive reviews)")
    
    if negative_pct > 30:
        report.append("⚠️  High negative sentiment detected - immediate action recommended")
    
    # Classes needing attention
    if 'Negative' in breakdown_pct.columns:
        high_negative_classes = breakdown_pct[breakdown_pct['Negative'] > 30].index.tolist()
        if high_negative_classes:
            report.append(f"\n⚠️  Classes needing immediate attention:")
            for cls in high_negative_classes:
                neg_pct = breakdown_pct.loc[cls, 'Negative']
                report.append(f"  • {cls}: {neg_pct:.1f}% negative")
    
    report_text = "\n".join(report)
    print(report_text)
    
    # Save to file
    with open('insights_report.txt', 'w', encoding='utf-8') as f:
        f.write(report_text)
    
    print("\n Report saved to: insights_report.txt")
    
    return report_text