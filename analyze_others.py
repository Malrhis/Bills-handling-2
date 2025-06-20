import sqlite3
import pandas as pd
from collections import Counter
import re

def analyze_others_category():
    conn = sqlite3.connect('data/expenses.db')
    
    # Get all expenses in 'others' category
    query = """
    SELECT description, COUNT(*) as count, SUM(amount) as total_amount
    FROM expenses 
    WHERE category = 'others' 
    GROUP BY description
    ORDER BY count DESC, total_amount DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    # Print analysis
    print("\nAnalysis of 'others' category:")
    print("-" * 50)
    print(f"Total unique descriptions: {len(df)}")
    print(f"Total transactions: {df['count'].sum()}")
    print(f"Total amount: ${df['total_amount'].sum():.2f}")
    
    # Extract common words
    words = []
    for desc in df['description']:
        if pd.isna(desc):
            continue
        # Split into words and clean
        desc_words = re.findall(r'\b\w+\b', desc.lower())
        words.extend(desc_words)
    
    # Count word frequency
    word_freq = Counter(words)
    
    # Print most common words
    print("\nMost common words in descriptions:")
    print("-" * 50)
    for word, count in word_freq.most_common(20):
        print(f"{word}: {count}")
    
    # Print all unique descriptions
    print("\nAll unique descriptions in 'others' category:")
    print("-" * 50)
    for _, row in df.iterrows():
        print(f"{row['description']}: {row['count']} times, ${row['total_amount']:.2f}")
    
    conn.close()

if __name__ == "__main__":
    analyze_others_category() 