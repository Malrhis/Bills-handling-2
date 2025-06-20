import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import sqlite3
import re
import json
from io import StringIO
from datetime import datetime
from pathlib import Path
from dateutil.parser import parse
# Import fuzzywuzzy with fallback for when Levenshtein is not available
try:
    from fuzzywuzzy import process
except ImportError:
    st.error("fuzzywuzzy not available. Please install it with: pip install fuzzywuzzy")
    process = None
import atexit

# Ensure the data directory exists
Path("data").mkdir(exist_ok=True)

# Add global connection tracking
_db_connections = []

def close_all_db_connections():
    """Close all tracked database connections"""
    global _db_connections
    for conn in _db_connections:
        try:
            conn.close()
        except:
            pass
    _db_connections = []

# Register the cleanup function
atexit.register(close_all_db_connections)

def parse_date(date_str):
    """Parse date from various formats to YYYY-MM-DD"""
    try:
        # Try different date formats
        for fmt in ['%d %b %Y', '%d%b%Y', '%Y-%m-%d']:
            try:
                return datetime.strptime(date_str.strip(), fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        # If none of the above work, try pandas to_datetime
        return pd.to_datetime(date_str).strftime('%Y-%m-%d')
    except Exception:
        return None

def format_date_for_db(date_obj):
    """Format a date object to YYYY-MM-DD string for database"""
    if isinstance(date_obj, str):
        return date_obj
    return date_obj.strftime('%Y-%m-%d')

def load_default_categories():
    """Load default categories from JSON file"""
    try:
        with open('default_categories.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error("Default categories file not found. Using empty categories.")
        return {}
    except json.JSONDecodeError:
        st.error("Error reading default categories file. Using empty categories.")
        return {}

def init_db():
    """Initialize database connection and create tables if not exists"""
    try:
        conn = sqlite3.connect('data/expenses.db')
        _db_connections.append(conn)  # Track the connection
        c = conn.cursor()
        
        # Create expenses table
        c.execute('''
            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE,
                description TEXT,
                amount REAL,
                category TEXT,
                self_percentage REAL,
                self_amount REAL,
                wife_amount REAL,
                original_text TEXT,
                credit_card_bill_month DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Add credit_card_bill_month column if it doesn't exist
        try:
            c.execute('ALTER TABLE expenses ADD COLUMN credit_card_bill_month DATE')
        except sqlite3.OperationalError:
            # Column already exists
            pass
        
        # Create categories table with version tracking
        c.execute('''
            CREATE TABLE IF NOT EXISTS categories (
                name TEXT PRIMARY KEY,
                keywords TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Check if categories table is empty
        c.execute('SELECT COUNT(*) FROM categories')
        if c.fetchone()[0] == 0:
            # Load and insert default categories
            default_categories = load_default_categories()
            for category, keywords in default_categories.items():
                c.execute('''
                    INSERT OR IGNORE INTO categories (name, keywords, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                ''', (category.lower(), ','.join(keywords)))
        
        conn.commit()
        return conn
    except Exception as e:
        st.error(f"Database initialization error: {str(e)}")
        return None

def get_all_categories():
    """Get all categories and their keywords from the database"""
    try:
        conn = init_db()
        if conn is None:
            return {}
        
        cursor = conn.cursor()
        cursor.execute('SELECT name, keywords FROM categories')
        categories = {}
        for name, keywords in cursor.fetchall():
            categories[name] = keywords.split(',') if keywords else []
        
        conn.close()
        return categories
    except Exception as e:
        st.error(f"Error getting categories: {str(e)}")
        return {}

def update_category(name, keywords):
    """Update a category with new keywords"""
    try:
        conn = init_db()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        keywords_str = ','.join(keywords) if keywords else ''
        cursor.execute('''
            INSERT OR REPLACE INTO categories (name, keywords, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (name.lower(), keywords_str))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error updating category: {str(e)}")
        return False

def delete_category(name):
    """Delete a category"""
    try:
        conn = init_db()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        cursor.execute('DELETE FROM categories WHERE name = ?', (name.lower(),))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error deleting category: {str(e)}")
        return False

def export_categories():
    """Export current categories to JSON file"""
    try:
        categories = get_all_categories()
        with open('default_categories.json', 'w') as f:
            json.dump(categories, f, indent=4)
        return True
    except Exception as e:
        st.error(f"Error exporting categories: {str(e)}")
        return False

def import_categories(file_content):
    """Import categories from JSON content"""
    try:
        categories = json.loads(file_content)
        conn = init_db()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        for category, keywords in categories.items():
            cursor.execute('''
                INSERT OR REPLACE INTO categories (name, keywords, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (category.lower(), ','.join(keywords)))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error importing categories: {str(e)}")
        return False

def add_expense(date, description, amount, category, self_percentage, self_amount, wife_amount, text, bill_month):
    """Add a new expense to the database"""
    try:
        conn = init_db()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO expenses (date, description, amount, category, self_percentage, 
                                self_amount, wife_amount, original_text, credit_card_bill_month)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            date, description, amount, category,
            self_percentage, self_amount, wife_amount, text,
            bill_month
        ))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error adding expense: {str(e)}")
        return False

def delete_expense(expense_id):
    """Delete an expense from the database"""
    try:
        conn = init_db()
        if conn is None:
            return False
        
        conn.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error deleting expense: {str(e)}")
        return False

def save_expenses(df):
    """Save new expenses to database"""
    try:
        conn = init_db()
        if conn is None:
            return False
        
        # First convert dates to proper format
        dates = []
        for date_str in df['Date']:
            parsed_date = parse_date(date_str)
            if parsed_date is None:
                st.error(f"Could not parse date: {date_str}")
                return False
            dates.append(parsed_date)
        
        # Format credit card bill month
        bill_month = df['Credit Card Bill Month'].iloc[0]  # All rows will have the same bill month
        
        for i, row in df.iterrows():
            conn.execute('''
                INSERT INTO expenses (date, description, amount, category, self_percentage, 
                                    self_amount, wife_amount, original_text, credit_card_bill_month)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                dates[i], row['Bills'], row['Amount'], row['Category'],
                row['Self Percentage'], row['Self Amount'], row['Wife Amount'], row['Text'],
                bill_month
            ))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error saving expenses: {str(e)}")
        return False

def load_expenses(start_date=None, end_date=None):
    """Load expenses from database with optional date filtering"""
    try:
        conn = init_db()
        if conn is None:
            return pd.DataFrame()
        
        query = "SELECT * FROM expenses"
        params = []
        if start_date and end_date:
            # Format dates consistently for SQLite
            start_str = format_date_for_db(start_date)
            end_str = format_date_for_db(end_date)
            query += " WHERE date >= ? AND date <= ?"
            params = [start_str, end_str]
        
        query += " ORDER BY date DESC"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Convert dates to datetime for display
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df['credit_card_bill_month'] = pd.to_datetime(df['credit_card_bill_month'])
        
        return df
    except Exception as e:
        st.error(f"Error loading expenses: {str(e)}")
        return pd.DataFrame()

def update_expense(expense_id, category=None, self_percentage=None):
    """Update an existing expense in the database"""
    conn = init_db()
    updates = []
    values = []
    if category is not None:
        updates.append("category = ?")
        values.append(category)
    if self_percentage is not None:
        updates.append("self_percentage = ?")
        values.append(self_percentage)
        # Recalculate amounts
        c = conn.cursor()
        c.execute("SELECT amount FROM expenses WHERE id = ?", (expense_id,))
        amount = c.fetchone()[0]
        self_amount = round(amount * (self_percentage / 100), 2)
        wife_amount = round(amount - self_amount, 2)
        updates.extend(["self_amount = ?", "wife_amount = ?"])
        values.extend([self_amount, wife_amount])
    
    if updates:
        values.append(expense_id)
        query = f"UPDATE expenses SET {', '.join(updates)} WHERE id = ?"
        conn.execute(query, values)
        conn.commit()
    conn.close()

def extract_amount(text):
    """Extract numerical amount from text containing S$xx.xx format, handling credits/reversals"""
    if pd.isna(text):
        return 0.0
    
    original_text = str(text).strip()
    text = original_text.lower()
    
    # Handle N/A or empty cases
    if text == '' or 'n/a' in text:
        return 0.0
    
    # Look for amount pattern
    amount_match = re.search(r'[Ss]\$(\d+(?:\.\d{2})?)', original_text)
    if amount_match:
        amount = float(amount_match.group(1))
        
        # Check for credit/reversal in lowercase text
        if 'cr' in text:
            amount = -amount
        
        return amount
    else:
        return 0.0

def test_amount_extraction():
    """Test function for amount extraction"""
    test_cases = [
        ("S$25.90", 25.90),
        ("s$25.90", 25.90),  # lowercase test
        ("S$25.90 cr", -25.90),
        ("S$25.90CR", -25.90),
        ("s$25.90 cr", -25.90),  # lowercase test
        ("MSIA CUISINE PTE LTD N/A SG", 0.0),
        ("MSIG SINGAPORE WWW.MSIG.COM. SG", 0.0),
        ("ZERO1 PTE LTD SINGAPORE SG", 0.0),
        ("POKKA PTE LTD SINGAPORE SG", 0.0),
        ("S$100", 100.0),
        ("s$100", 100.0),  # lowercase test
        ("Payment received S$50.00", 50.0),
        ("Credit S$75.50 cr", -75.50),
        ("Transaction s$30.00", 30.0),  # lowercase test
    ]
    
    results = []
    for input_text, expected in test_cases:
        actual = extract_amount(input_text)
        passed = abs(actual - expected) < 0.01  # Using small epsilon for float comparison
        results.append({
            'Input': input_text,
            'Expected': expected,
            'Actual': actual,
            'Passed': passed
        })
    
    return pd.DataFrame(results)

def categorize_expense(expense_name):
    """Categorize expense based on fuzzy matching of keywords in the expense name"""
    if pd.isna(expense_name):
        return 'others', 0
    
    # Clean and normalize the expense name
    expense_name = str(expense_name).lower()
    
    # Replace special characters with space, but preserve alphanumeric, spaces, and hyphens
    # First, replace standalone hyphens (hyphens with spaces around them)
    expense_name = re.sub(r'\s+-\s+', ' ', expense_name)
    # Then clean other special characters but preserve hyphens between words
    expense_name = re.sub(r'[^a-z0-9\s\-]', ' ', expense_name)
    # Normalize whitespace
    expense_name = ' '.join(word.strip() for word in expense_name.split())
    
    if not expense_name:  # If after cleaning the string is empty
        return 'others', 0
    
    categories = get_all_categories()
    
    # Check if fuzzywuzzy is available
    if process is not None:
        # Use fuzzy matching
        words = [w for w in re.split(r'[\s-]+', expense_name) if w]
        best_category = 'others'
        highest_score = 0
        
        for word in words:
            if not word:  # Skip empty words
                continue
            for category, keywords in categories.items():
                if not keywords:  # Skip empty keyword lists
                    continue
                # Find the best matching keyword for this word
                try:
                    best_match, score = process.extractOne(word, keywords)
                    if score > highest_score and score > 75:  # 75% similarity threshold
                        highest_score = score
                        best_category = category
                except Exception:
                    continue  # Skip if there's an error processing this word
        
        return best_category, highest_score
    else:
        # Fallback to simple string matching
        best_category = 'others'
        highest_score = 0
        
        for category, keywords in categories.items():
            if not keywords:  # Skip empty keyword lists
                continue
            for keyword in keywords:
                if keyword and keyword.strip():
                    keyword = keyword.strip().lower()
                    if keyword in expense_name:
                        # Simple exact match - give it a score of 100
                        return category, 100
        
        return best_category, highest_score

def calculate_split_amounts(amount, self_percentage):
    """Calculate self and wife amounts based on percentage split"""
    self_amount = round(amount * (self_percentage / 100), 2)
    wife_amount = round(amount - self_amount, 2)
    return self_amount, wife_amount

def find_duplicate_keywords():
    """Find keywords that appear in multiple categories"""
    categories = get_all_categories()
    keyword_map = {}  # maps keyword to list of categories it appears in
    
    for category, keywords in categories.items():
        for keyword in keywords:
            if keyword and keyword.strip():  # Skip empty keywords
                keyword = keyword.strip().lower()
                if keyword not in keyword_map:
                    keyword_map[keyword] = []
                keyword_map[keyword].append(category)
    
    # Return only keywords that appear in multiple categories
    return {k: v for k, v in keyword_map.items() if len(v) > 1}

def update_categories_with_unique_keywords(edited_categories):
    """Update categories while ensuring keywords are unique across categories"""
    try:
        # First, process all keywords and build a map of which keywords are used where
        keyword_map = {}  # maps keyword to (category, position in list)
        processed_categories = {}
        
        for category, keywords_str in edited_categories.items():
            keywords = [k.strip().lower() for k in keywords_str.split(',') if k.strip()]
            processed_categories[category] = []
            
            for keyword in keywords:
                if keyword in keyword_map:
                    # Skip duplicate keyword - it belongs to another category
                    continue
                keyword_map[keyword] = category
                processed_categories[category].append(keyword)
        
        # Now save the processed categories to the database
        conn = init_db()
        if conn is None:
            return False, "Database connection failed"
        
        success = True
        duplicates_found = False
        for category, keywords in processed_categories.items():
            keywords_str = ','.join(keywords) if keywords else ''
            try:
                conn.execute('INSERT OR REPLACE INTO categories (name, keywords) VALUES (?, ?)',
                          (category.lower(), keywords_str))
            except Exception as e:
                success = False
                conn.close()
                return False, f"Error updating category {category}: {str(e)}"
        
        conn.commit()
        conn.close()
        
        return True, "Categories updated successfully"
    except Exception as e:
        return False, f"Error updating categories: {str(e)}"

def update_existing_expenses_categories():
    """Update categories of existing expenses based on current category settings"""
    try:
        # Close all existing connections first
        close_all_db_connections()
        
        # Create a new connection for the update process
        conn = sqlite3.connect('data/expenses.db')
        cursor = conn.cursor()
        
        # Get all expenses
        cursor.execute('SELECT id, description, category FROM expenses')
        expenses = cursor.fetchall()
        
        # Track statistics
        total_updates = 0
        updated_expenses = []
        
        # Update each expense
        for expense_id, description, current_category in expenses:
            if description:
                new_category, confidence = categorize_expense(description)
                if new_category != current_category:  # Only update if category has changed
                    print(f"Updating expense ID {expense_id}: '{description}' from '{current_category}' to '{new_category}' (confidence: {confidence})")
                    cursor.execute(
                        'UPDATE expenses SET category = ? WHERE id = ?',
                        (new_category, expense_id)
                    )
                    if cursor.rowcount > 0:
                        total_updates += 1
                        updated_expenses.append((expense_id, description, new_category))
                else:
                    print(f"No update needed for expense ID {expense_id} (already in category '{current_category}')")
        
        conn.commit()
        conn.close()
        
        return True, {
            'total_processed': len(expenses),
            'total_updated': total_updates,
            'updated_expenses': updated_expenses
        }
    except Exception as e:
        print(f"Error updating existing expenses: {str(e)}")
        return False, f"Error updating existing expenses: {str(e)}"
    finally:
        try:
            conn.close()
        except:
            pass

# Set page config
st.set_page_config(page_title="Bills Allocation App", layout="wide")

# Title
st.title("Bills Allocation App")

# Sidebar for navigation
page = st.sidebar.radio("Navigation", ["Add New Expenses", "View/Edit History", "Category Settings"])

if page == "Category Settings":
    st.write("### Category Management")
    st.write("Manage your expense categories and their associated keywords.")
    
    # Add import/export section
    st.write("#### Import/Export Categories")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Export Current Categories"):
            if export_categories():
                st.success("✅ Categories exported to default_categories.json")
            else:
                st.error("❌ Failed to export categories")
    
    with col2:
        uploaded_file = st.file_uploader("Import Categories from JSON", type=['json'])
        if uploaded_file is not None:
            content = uploaded_file.read().decode()
            if import_categories(content):
                st.success("✅ Categories imported successfully")
                st.rerun()
            else:
                st.error("❌ Failed to import categories")
    
    # Get current categories
    categories = get_all_categories()
    
    # Check for duplicate keywords
    duplicates = find_duplicate_keywords()
    if duplicates:
        st.warning("⚠️ The following keywords appear in multiple categories:")
        for keyword, categories_list in duplicates.items():
            st.write(f"- '{keyword}' appears in: {', '.join(categories_list)}")
        st.write("Keywords should be unique to each category for better categorization.")
    
    # Add new category section
    st.write("#### Add New Category")
    col1, col2 = st.columns([1, 2])
    with col1:
        new_category = st.text_input("Category Name")
    with col2:
        new_keywords = st.text_input("Keywords (comma-separated)",
                                   help="Add keywords that help identify this category")
    
    if st.button("Add Category") and new_category:
        keywords_list = [k.strip().lower() for k in new_keywords.split(',')] if new_keywords else []
        if update_category(new_category, keywords_list):
            st.success(f"✅ Added category: {new_category}")
            st.rerun()
    
    # Edit existing categories section
    st.write("#### Edit Existing Categories")
    st.write("Edit keywords for each category. Separate keywords with commas. Keywords should be unique across categories.")
    
    edited_categories = {}
    changes_made = False
    
    for category, keywords in sorted(categories.items()):
        st.write(f"**{category}**")
        current_keywords = ','.join(keywords) if keywords else ''
        new_value = st.text_area(f"Keywords for {category}", 
                                value=current_keywords,
                                key=f"cat_{category}",
                                height=100)
        
        if new_value != current_keywords:
            edited_categories[category] = new_value
            changes_made = True
    
    if changes_made:
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("Save Changes"):
                success, message = update_categories_with_unique_keywords(edited_categories)
                if success:
                    st.success(f"✅ {message}")
                    st.rerun()
                else:
                    st.error(f"❌ {message}")
    
    # Add section for updating existing expenses
    st.write("#### Update Existing Expenses")
    st.write("Update the categories of all existing expenses based on current category settings.")
    st.write("⚠️ This will re-categorize expenses based on the current keywords.")
    st.write("⚠️ During the update, all other database operations will be paused.")
    
    if st.button("Update Existing Expenses"):
        with st.spinner("Updating categories... Please wait."):
            success, result = update_existing_expenses_categories()
            if success:
                st.success(f"✅ Successfully processed {result['total_processed']} expenses.")
                st.write(f"Updated {result['total_updated']} expenses with new categories.")
                
                if result['total_updated'] > 0:
                    st.write("#### Updated Expenses:")
                    updates_df = pd.DataFrame(
                        result['updated_expenses'],
                        columns=['ID', 'Description', 'New Category']
                    )
                    st.dataframe(updates_df)
            else:
                st.error(f"❌ {result}")

elif page == "Add New Expenses":
    st.write("### Add New Expenses")
    
    # Add tabs for different input methods
    input_method = st.tabs(["Single Entry", "Bulk Import"])
    
    with input_method[0]:  # Single Entry
        st.write("Add a single expense entry")
        
        # Create a form for single expense entry
        with st.form("single_expense_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                expense_date = st.date_input(
                    "Date",
                    value=datetime.now(),
                    help="Date of the expense"
                )
                expense_description = st.text_input(
                    "Description",
                    help="Description of the expense"
                )
                expense_amount = st.number_input(
                    "Amount",
                    min_value=0.0,
                    format="%.2f",
                    help="Total amount of the expense"
                )
            
            with col2:
                # Get available categories for dropdown
                available_categories = sorted(get_all_categories().keys())
                expense_category = st.selectbox(
                    "Category",
                    options=available_categories,
                    help="Select the expense category"
                )
                expense_split = st.number_input(
                    "Your Share %",
                    min_value=0,
                    max_value=100,
                    value=50,
                    help="Your percentage share of the expense (50 means 50-50 split)"
                )
                bill_month = st.date_input(
                    "Credit Card Bill Month",
                    value=datetime.now().replace(day=1),
                    help="Select which credit card bill month this expense belongs to"
                )
            
            # Calculate split amounts
            self_amount, wife_amount = calculate_split_amounts(expense_amount, expense_split)
            
            # Show split preview
            st.write("#### Split Preview")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Your Amount", f"${self_amount:.2f}")
            with col2:
                st.metric("Wife's Amount", f"${wife_amount:.2f}")
            with col3:
                st.metric("Total Amount", f"${expense_amount:.2f}")
            
            submitted = st.form_submit_button("Add Expense")
            
            if submitted and expense_description and expense_amount > 0:
                # Create a single-row DataFrame
                df = pd.DataFrame({
                    'Date': [expense_date],
                    'Bills': [expense_description],
                    'Amount': [expense_amount],
                    'Category': [expense_category],
                    'Self Percentage': [expense_split],
                    'Self Amount': [self_amount],
                    'Wife Amount': [wife_amount],
                    'Text': [f"S${expense_amount:.2f}"],  # Create a simple text representation
                    'Credit Card Bill Month': [bill_month]
                })
                
                if save_expenses(df):
                    st.success("✅ Expense saved successfully!")
                    # Clear the form by rerunning
                    st.rerun()
                else:
                    st.error("❌ Failed to save expense")
    
    with input_method[1]:  # Bulk Import
        st.write("Import multiple expenses from table data")
        
        # Add credit card bill month selector
        bill_month = st.date_input(
            "Credit Card Bill Month",
            value=datetime.now().replace(day=1),
            help="Select which credit card bill month these expenses belong to"
        )

        st.write("Paste your table data below:")
        
        # Add test section
        with st.expander("Debug: Test Amount Extraction"):
            st.write("Testing amount extraction with various cases:")
            test_results = test_amount_extraction()
            st.dataframe(test_results)
        
        table_data = st.text_area("Table Data", height=200)

        if table_data:
            try:
                # Convert pasted data to DataFrame
                df = pd.read_csv(StringIO(table_data), sep='\t', header=None)
                df.columns = ['Date', 'Bills', 'Text']
                
                # Add credit card bill month to all rows
                df['Credit Card Bill Month'] = bill_month.strftime('%Y-%m-%d')
                
                # Process the data
                df['Amount'] = df['Text'].apply(extract_amount)
                
                # Get categories and confidence scores
                categories_and_scores = [categorize_expense(bill) for bill in df['Bills']]
                df['Category'] = [cat for cat, score in categories_and_scores]
                df['Category_Confidence'] = [score for cat, score in categories_and_scores]
                
                # Validate dates before proceeding
                invalid_dates = []
                for date_str in df['Date']:
                    if parse_date(date_str) is None:
                        invalid_dates.append(date_str)
                
                if invalid_dates:
                    st.error(f"Invalid date format found in: {', '.join(invalid_dates)}")
                else:
                    # First show the category review section
                    st.write("### Category Review")
                    st.write("Review and customize categories for expenses. You can create new categories in the Category Settings page.")
                    
                    # Get all available categories for the dropdown
                    available_categories = sorted(get_all_categories().keys())
                    
                    # Show expenses with low confidence scores first
                    review_df = df[['Bills', 'Amount', 'Category', 'Category_Confidence']].copy()
                    review_df = review_df.sort_values('Category_Confidence')
                    
                    edited_categories = st.data_editor(
                        review_df,
                        column_config={
                            "Bills": "Expense Name",
                            "Amount": st.column_config.NumberColumn("Amount", format="%.2f", disabled=True),
                            "Category": st.column_config.SelectboxColumn(
                                "Category",
                                options=available_categories,
                                help="Select a category"
                            ),
                            "Category_Confidence": st.column_config.NumberColumn(
                                "Confidence Score",
                                format="%.0f%%",
                                help="How confident the system is about the suggested category",
                                disabled=True
                            )
                        },
                        hide_index=True,
                        key="category_editor"
                    )
                    
                    # Update categories in the main dataframe
                    df['Category'] = edited_categories['Category']

                    # Split Entry Section
                    st.write("### Split Entry")
                    st.write("Enter the split percentage for each expense. 50 means 50-50 split.")
                    
                    # Create a session state for storing splits if it doesn't exist
                    if 'splits_entered' not in st.session_state:
                        st.session_state.splits_entered = False
                        st.session_state.splits_data = None

                    # Display expenses for split entry
                    splits_df = df[['Date', 'Bills', 'Amount', 'Category']].copy()
                    splits_df['Self Percentage'] = 50.0  # Default value
                    
                    edited_splits = st.data_editor(
                        splits_df,
                        column_config={
                            "Date": st.column_config.DateColumn("Date", disabled=True),
                            "Bills": st.column_config.TextColumn("Expense Name", disabled=True),
                            "Amount": st.column_config.NumberColumn("Total Amount", format="%.2f", disabled=True),
                            "Category": st.column_config.TextColumn("Category", disabled=True),
                            "Self Percentage": st.column_config.NumberColumn(
                                "Your Share %",
                                min_value=0,
                                max_value=100,
                                step=1,
                                format="%.0f"
                            )
                        },
                        hide_index=True,
                        key="splits_editor"
                    )

                    if st.button("Confirm Splits"):
                        # Store the splits data in session state
                        st.session_state.splits_data = edited_splits
                        st.session_state.splits_entered = True
                        st.rerun()

                    # Only show the Expense Allocation section if splits have been entered
                    if st.session_state.splits_entered and st.session_state.splits_data is not None:
                        st.write("### Expense Allocation Review")
                        st.write("Review the complete allocation before saving to database.")
                        
                        # Create the final allocation dataframe
                        final_df = df.copy()
                        final_df['Self Percentage'] = st.session_state.splits_data['Self Percentage']
                        final_df['Self Amount'] = final_df.apply(
                            lambda row: calculate_split_amounts(row['Amount'], row['Self Percentage'])[0], 
                            axis=1
                        )
                        final_df['Wife Amount'] = final_df.apply(
                            lambda row: calculate_split_amounts(row['Amount'], row['Self Percentage'])[1], 
                            axis=1
                        )

                        # Display final allocation for review
                        st.data_editor(
                            final_df,
                            column_config={
                                "Date": st.column_config.DateColumn("Date", disabled=True),
                                "Bills": st.column_config.TextColumn("Expense Name", disabled=True),
                                "Text": st.column_config.TextColumn("Original Amount Text", disabled=True),
                                "Amount": st.column_config.NumberColumn("Total Amount", format="%.2f", disabled=True),
                                "Category": st.column_config.TextColumn("Category", disabled=True),
                                "Self Percentage": st.column_config.NumberColumn("Your Share %", format="%.0f", disabled=True),
                                "Self Amount": st.column_config.NumberColumn("Your Amount", format="%.2f", disabled=True),
                                "Wife Amount": st.column_config.NumberColumn("Wife Amount", format="%.2f", disabled=True)
                            },
                            hide_index=True,
                            key="final_allocation_review"
                        )

                        # Display totals
                        total_amount = final_df['Amount'].sum()
                        self_total = final_df['Self Amount'].sum()
                        wife_total = final_df['Wife Amount'].sum()
                        
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Amount", f"${total_amount:.2f}")
                        with col2:
                            st.metric("Your Total", f"${self_total:.2f}")
                        with col3:
                            st.metric("Wife Total", f"${wife_total:.2f}")
                        with col4:
                            overall_self_percentage = (self_total / total_amount * 100) if total_amount > 0 else 0
                            st.metric("Your Overall Share", f"{overall_self_percentage:.1f}%")

                        if st.button("Save Expenses"):
                            if save_expenses(final_df):
                                st.success("✅ Expenses saved successfully!")
                                # Show how many records were saved
                                st.info(f"📝 {len(final_df)} records saved to database")
                                # Reset the session state
                                st.session_state.splits_entered = False
                                st.session_state.splits_data = None
                                st.rerun()
                            else:
                                st.error("❌ Failed to save expenses")

                        if st.button("Edit Splits"):
                            st.session_state.splits_entered = False
                            st.session_state.splits_data = None
                            st.rerun()

            except Exception as e:
                st.error(f"Error processing the data: {str(e)}")

elif page == "View/Edit History":
    st.write("### View and Edit Historical Expenses")
    
    # Get current month's start and end dates
    today = datetime.now()
    start_of_month = today.replace(day=1)
    
    # Date range selector
    st.write("Select Date Range")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=start_of_month,
            min_value=datetime(2000, 1, 1),
            key="start_date"
        )
    with col2:
        # For end date, allow selecting up to the end of the current year
        end_date = st.date_input(
            "End Date",
            value=today,
            min_value=start_date,
            max_value=datetime(today.year, 12, 31),
            key="end_date"
        )
    
    # Add filter by credit card bill month
    filter_by_bill_month = st.checkbox("Filter by Credit Card Bill Month")
    if filter_by_bill_month:
        bill_month_filter = st.date_input(
            "Credit Card Bill Month",
            value=start_of_month,
            key="bill_month_filter"
        )
    
    # Load and display historical data
    hist_df = load_expenses(start_date, end_date)
    
    if hist_df.empty:
        st.info("No expenses found for the selected date range. Try adding some expenses in the 'Add New Expenses' section.")
    else:
        # Apply bill month filter if selected
        if filter_by_bill_month and not hist_df.empty:
            bill_month_str = bill_month_filter.strftime('%Y-%m')
            hist_df = hist_df[hist_df['credit_card_bill_month'].dt.strftime('%Y-%m') == bill_month_str]
            if hist_df.empty:
                st.info("No expenses found for the selected credit card bill month.")
                st.stop()  # Stop the app here instead of return

        # Show basic statistics
        st.write(f"Found {len(hist_df)} expenses")

        # Add category-wise bar graph first
        st.write("### Expense Distribution by Category")
        
        # Prepare data for the graph
        category_summary = hist_df.groupby('category').agg({
            'amount': 'sum',
            'self_amount': 'sum',
            'wife_amount': 'sum'
        }).round(2)
        
        # Create a DataFrame in the format needed for grouped bar chart
        plot_data = []
        for category in category_summary.index:
            plot_data.extend([
                {
                    'Category': category,
                    'Amount': category_summary.loc[category, 'amount'],
                    'Type': 'Total'
                },
                {
                    'Category': category,
                    'Amount': category_summary.loc[category, 'self_amount'],
                    'Type': 'Your Share'
                },
                {
                    'Category': category,
                    'Amount': category_summary.loc[category, 'wife_amount'],
                    'Type': "Wife's Share"
                }
            ])
        plot_df = pd.DataFrame(plot_data)
        
        # Create the bar graph
        fig_category = px.bar(
            plot_df,
            x='Category',
            y='Amount',
            color='Type',
            title='Expense Distribution by Category',
            barmode='group',
            labels={'Amount': 'Amount ($)', 'Category': 'Expense Category'},
            color_discrete_map={
                'Total': '#1f77b4',
                'Your Share': '#2ca02c',
                "Wife's Share": '#ff7f0e'
            }
        )
        
        # Customize the layout
        fig_category.update_layout(
            xaxis_title="Category",
            yaxis_title="Amount ($)",
            legend_title="Type",
            height=500,
            bargap=0.2,
            bargroupgap=0.1
        )
        
        # Display the graph
        st.plotly_chart(fig_category, use_container_width=True)

        # Add stacked bar graph by credit card bill month
        st.write("### Expense Distribution by Month")
        
        # Toggle for x-axis type
        x_axis_type = st.radio("Select X-axis type:", ("Credit Card Bill Month", "Date Month"))
        
        # Prepare data for the graph
        if x_axis_type == "Credit Card Bill Month":
            # Ensure we have valid datetime objects
            hist_df['credit_card_bill_month'] = pd.to_datetime(hist_df['credit_card_bill_month'])
            # Extract month name and year from credit card bill month
            hist_df['month'] = hist_df['credit_card_bill_month'].dt.strftime('%B %Y')
        else:
            # Ensure we have valid datetime objects
            hist_df['date'] = pd.to_datetime(hist_df['date'])
            # Extract month name and year from transaction date
            hist_df['month'] = hist_df['date'].dt.strftime('%B %Y')
        
        # Sort the data chronologically by the original dates
        month_summary = hist_df.groupby(['month', 'category']).agg({'amount': 'sum'}).reset_index()
        
        # Create sorting keys based on the original datetime columns
        if x_axis_type == "Credit Card Bill Month":
            sort_keys = hist_df.groupby('month')['credit_card_bill_month'].first().dt.strftime('%Y%m')
        else:
            sort_keys = hist_df.groupby('month')['date'].first().dt.strftime('%Y%m')
        
        # Apply the sort to month_summary
        month_summary['sort_key'] = month_summary['month'].map(sort_keys)
        month_summary = month_summary.sort_values('sort_key')
        month_summary = month_summary.drop('sort_key', axis=1)
        
        # Create a stacked bar graph
        fig_month = px.bar(
            month_summary,
            x='month',
            y='amount',
            color='category',
            title=f'Expense Distribution by {x_axis_type}',
            labels={'amount': 'Amount ($)', 'month': x_axis_type},
            barmode='stack'
        )
        
        # Customize the layout
        fig_month.update_layout(
            xaxis_title=x_axis_type,
            yaxis_title="Amount ($)",
            legend_title="Category",
            height=500,
            bargap=0.2,
            bargroupgap=0.1,
            xaxis_tickangle=-45  # Angle the x-axis labels for better readability
        )
        
        # Display the graph
        st.plotly_chart(fig_month, use_container_width=True)

        # Now show the data editor with delete functionality
        st.write("### Detailed Expenses")
        st.write("Use the checkboxes to select expenses for deletion.")
        
        # Add search functionality
        search_col1, search_col2 = st.columns([3, 1])
        with search_col1:
            search_query = st.text_input("🔍 Search in Description or Category", "")
        with search_col2:
            search_type = st.selectbox("Search in", ["Description", "Category", "Both"])
        
        # Add sorting options
        sort_col1, sort_col2, sort_col3 = st.columns([2, 2, 1])
        with sort_col1:
            sort_by = st.selectbox(
                "Sort by",
                ["date", "description", "amount", "category", "self_percentage", "self_amount", "wife_amount"],
                index=0
            )
        with sort_col2:
            sort_order = st.selectbox("Sort order", ["Descending", "Ascending"], index=0)
        
        # Apply sorting
        ascending = sort_order == "Ascending"
        hist_df = hist_df.sort_values(by=sort_by, ascending=ascending)
        
        # Filter the DataFrame based on search query
        if search_query:
            search_query = search_query.lower()
            if search_type == "Description":
                hist_df = hist_df[hist_df['description'].str.lower().str.contains(search_query, na=False)]
            elif search_type == "Category":
                hist_df = hist_df[hist_df['category'].str.lower().str.contains(search_query, na=False)]
            else:  # Both
                hist_df = hist_df[
                    hist_df['description'].str.lower().str.contains(search_query, na=False) |
                    hist_df['category'].str.lower().str.contains(search_query, na=False)
                ]
            
            if hist_df.empty:
                st.info("No expenses found matching your search.")
                st.stop()
            else:
                st.success(f"Found {len(hist_df)} matching expenses.")
        
        # Add select all checkbox
        select_all = st.checkbox("Select All Expenses")
        
        # Add a delete button column to the DataFrame
        hist_df['Delete'] = select_all
        
        edited_hist_df = st.data_editor(
            hist_df,
            column_config={
                "Delete": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select rows to delete",
                    default=False,
                ),
                "id": st.column_config.NumberColumn("ID"),
                "date": st.column_config.DateColumn("Date"),
                "description": st.column_config.TextColumn("Description"),
                "amount": st.column_config.NumberColumn("Amount", format="%.2f"),
                "category": st.column_config.SelectboxColumn(
                    "Category",
                    options=sorted(get_all_categories().keys()),
                    help="Select a category"
                ),
                "self_percentage": st.column_config.NumberColumn("Your Share %", format="%.0f"),
                "self_amount": st.column_config.NumberColumn("Your Amount", format="%.2f"),
                "wife_amount": st.column_config.NumberColumn("Wife Amount", format="%.2f"),
                "credit_card_bill_month": st.column_config.DateColumn(
                    "Credit Card Bill Month",
                    help="The credit card statement month this expense belongs to"
                ),
                "original_text": st.column_config.TextColumn("Original Text"),
                "created_at": st.column_config.DateColumn("Created At")
            },
            column_order=["Delete", "id", "date", "description", "amount", "category", 
                         "self_percentage", "self_amount", "wife_amount", 
                         "credit_card_bill_month", "original_text", "created_at"],
            disabled=["id", "date", "description", "amount", "self_amount", "wife_amount", "original_text", "created_at"],
            hide_index=True,
            key="expense_editor",
            num_rows="dynamic",
            use_container_width=True,
            height=400
        )

        # Handle deletes
        rows_to_delete = edited_hist_df[edited_hist_df['Delete']]['id'].tolist()
        if rows_to_delete:
            delete_col1, delete_col2 = st.columns([3, 1])
            with delete_col1:
                st.warning(f"⚠️ You have selected {len(rows_to_delete)} expense(s) to delete.")
            with delete_col2:
                if st.button("Delete Selected", type="primary"):
                    success_count = 0
                    for expense_id in rows_to_delete:
                        if delete_expense(expense_id):
                            success_count += 1
                    
                    if success_count == len(rows_to_delete):
                        st.success(f"✅ Successfully deleted {success_count} expense(s)")
                    else:
                        st.warning(f"⚠️ Deleted {success_count} out of {len(rows_to_delete)} expenses")
                    st.rerun()
        
        # Handle updates
        if not edited_hist_df.equals(hist_df):
            for idx, row in edited_hist_df.iterrows():
                orig_row = hist_df.loc[idx]
                if (row['category'] != orig_row['category'] or 
                    row['self_percentage'] != orig_row['self_percentage']):
                    update_expense(
                        row['id'],
                        category=row['category'],
                        self_percentage=row['self_percentage']
                    )
            st.success("Updates saved!")
            st.rerun() 