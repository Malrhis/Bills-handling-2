# Bills Allocation App

This is a Streamlit application that helps you process and allocate bills between yourself and your wife, with automatic categorization of expenses.

## Features

- Paste tab-separated table data with Date, Bills (Expense Name), and Text Amount columns
- Automatic extraction of numerical amounts from text (S$xx.xx format)
- AI-based categorization of expenses based on expense names
- Ability to allocate amounts between Self and Wife
- Real-time validation of allocations
- Category-wise breakdown of expenses
- Total summaries for all parties

## Setup and Running

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
streamlit run app.py
```

## How to Use

1. Copy your table data (must be tab-separated)
2. Paste it into the text area in the application
3. The app will automatically process the data and show you an editable table
4. Enter the amounts for Self and Wife (they should sum up to the total amount)
5. The category will be automatically assigned, but you can change it if needed
6. View the totals and category-wise breakdown at the bottom

## Data Format

Your input data should have these columns:
- Date
- Bills (Expense Name)
- Text (Amount in S$xx.xx format)

Example:
```
Date    Bills    Text
2 Jan 2021    TRANS EUROKARS PL MAZD SINGAPORE SG    S$23.54
02 Jan 2021    MISATO SINGAPORE SG    S$77.80
3 Jan 2021    GONGYUAN MALATANG-CCP SINGAPORE SG    S$22.70
``` 