import sqlite3
import json
from fuzzywuzzy import process

def get_all_categories():
    """Get all categories and their keywords from the database"""
    try:
        conn = sqlite3.connect('data/expenses.db')
        cursor = conn.cursor()
        cursor.execute('SELECT name, keywords FROM categories')
        categories = {}
        for name, keywords in cursor.fetchall():
            categories[name] = keywords.split(',') if keywords else []
        conn.close()
        return categories
    except Exception as e:
        print(f"Error getting categories: {str(e)}")
        return {}

def categorize_expense(expense_name):
    """Categorize expense based on fuzzy matching of keywords in the expense name"""
    if not expense_name:
        return 'others', 0
    
    expense_name = str(expense_name).lower()
    categories = get_all_categories()
    
    # For each word in the expense name, find the best matching category
    words = expense_name.split()
    best_category = 'others'
    highest_score = 0
    
    for word in words:
        for category, keywords in categories.items():
            if not keywords:  # Skip empty keyword lists
                continue
            # Find the best matching keyword for this word
            best_match, score = process.extractOne(word, keywords)
            if score > highest_score and score > 75:  # 75% similarity threshold
                highest_score = score
                best_category = category
    
    return best_category, highest_score

def update_existing_expenses():
    """Update categories of existing expenses based on current category settings"""
    try:
        conn = sqlite3.connect('data/expenses.db')
        cursor = conn.cursor()
        
        # Get all expenses
        cursor.execute('SELECT id, description FROM expenses')
        expenses = cursor.fetchall()
        
        # Track statistics
        total_updates = 0
        updated_expenses = []
        
        # Update each expense
        for expense_id, description in expenses:
            if description:
                new_category, confidence = categorize_expense(description)
                cursor.execute(
                    'UPDATE expenses SET category = ? WHERE id = ? AND category != ?',
                    (new_category, expense_id, new_category)
                )
                if cursor.rowcount > 0:
                    total_updates += 1
                    updated_expenses.append((expense_id, description, new_category))
        
        conn.commit()
        conn.close()
        
        return True, {
            'total_processed': len(expenses),
            'total_updated': total_updates,
            'updated_expenses': updated_expenses
        }
    except Exception as e:
        return False, f"Error updating existing expenses: {str(e)}"

def sync_categories_from_json():
    """Sync categories in database with default_categories.json"""
    try:
        # Read categories from JSON
        with open('default_categories.json', 'r') as f:
            categories = json.load(f)
        
        # Connect to database
        conn = sqlite3.connect('data/expenses.db')
        cursor = conn.cursor()
        
        # Clear existing categories
        cursor.execute('DELETE FROM categories')
        
        # Insert categories from JSON
        for category, keywords in categories.items():
            cursor.execute(
                'INSERT INTO categories (name, keywords) VALUES (?, ?)',
                (category.lower(), ','.join(keywords))
            )
        
        conn.commit()
        conn.close()
        return True, "Categories synced successfully from JSON"
    except Exception as e:
        return False, f"Error syncing categories: {str(e)}"

def main():
    print("Syncing categories from JSON...")
    success, message = sync_categories_from_json()
    if not success:
        print(f"Error: {message}")
        return
    
    print("Updating expense categories...")
    success, result = update_existing_expenses()
    
    if success:
        print(f"\nResults:")
        print(f"Total expenses processed: {result['total_processed']}")
        print(f"Expenses updated: {result['total_updated']}")
        
        if result['total_updated'] > 0:
            print("\nUpdated expenses:")
            print("-" * 50)
            for expense_id, description, new_category in result['updated_expenses']:
                print(f"{description} -> {new_category}")
    else:
        print(f"Error: {result}")

if __name__ == "__main__":
    main() 