# DataTrack

BLG-317E Database Systems Project - Sakila Film Rental Admin Panel

## About

A Flask-based web application for managing a Film rental store database (Sakila). Provides CRUD operations and analytics for films, customers, addresses, payments, and rentals.

## Features

- **Films**: Browse, add, edit, delete films. View film statistics by category, actor, and rating.
- **Customers**: Manage customer records. View top spenders.
- **Addresses**: Manage addresses. View top countries by customer count and spending.
- **Payments**: Track payments with filtering and sorting. Add, edit, delete payments. View analytics.
- **Rentals**: Manage rental orders. Track returns. View top rented films.

## Tech Stack

- **Backend**: Python, Flask
- **Database**: MySQL (Sakila sample database)
- **Frontend**: HTML, Bootstrap 5, Bootstrap Icons

## Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure database connection in `settings.py`
4. Run the application:
   ```bash
   python3 app.py
   ```
5. Open `http://localhost:5000` in your browser

## Project Structure

```
DataTrack/
├── app.py                 # Flask routes
├── settings.py            # Database configuration
├── utils/
│   └── table_operations.py   # Database queries
├── templates/             # HTML templates
├── static/css/            # Stylesheets
└── Data/                  # SQL data files
```

## Team

- Tunahan Geçit
- Alperen Yılmaz
- Cengizhan Kırpık
- Emir Buğra Şahin
- Muhammed Yunus Doğru
