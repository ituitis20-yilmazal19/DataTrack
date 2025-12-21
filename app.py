from flask import Flask, render_template, request, redirect, url_for, flash
from settings import db_user, db_password, db_host, db_name
import mysql.connector
from utils.table_operations import Films, Customers, Addresses, Payments, Rentals
import math

app = Flask(__name__)
app.secret_key = "dev-only-change-me"

def get_connection():
    return mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        charset="utf8mb4",
        autocommit=True,
    )

# Sınıfları başlat
films = Films(connection_factory=get_connection)
customers = Customers(connection_factory=get_connection)
addresses = Addresses(connection_factory=get_connection)
payments = Payments(connection_factory=get_connection)
rentals = Rentals(connection_factory=get_connection)

@app.route("/")
def main():
    return render_template("main.html")

# --- FILMS ---
@app.route("/films")
def films_list():
    category_id = request.args.get("category_id", type=int)
    language_id = request.args.get("language_id", type=int)
    q = request.args.get("q", type=str)
    page = max(request.args.get("page", default=1, type=int), 1)
    page_size = 20
    rows = films.search(category_id=category_id, language_id=language_id, q=q,
                        page=page, page_size=page_size)
    total_count = films.count_search(category_id=category_id, language_id=language_id, q=q)
    total_pages = math.ceil(total_count / page_size)
    languages = films.languages()
    categories = films.categories()

    return render_template("films.html",
                           films=rows,
                           languages=languages,
                           categories=categories,
                           sel_category_id=category_id,
                           sel_language_id=language_id,
                           q=q,
                           page=page,
                           total_pages=total_pages)

@app.route("/film/<int:film_id>", methods=["GET", "POST"])
def film_detail(film_id):
    if request.method == "POST":
        payload = {
            "title": request.form.get("title"),
            "description": request.form.get("description"),
            "release_year": request.form.get("release_year", type=int),
            "language_id": request.form.get("language_id", type=int),
            "rating": request.form.get("rating"),
            "rental_rate": request.form.get("rental_rate", type=float),
            "length": request.form.get("length", type=int),
            "replacement_cost": request.form.get("replacement_cost", type=float),
            "rental_duration": request.form.get("rental_duration", type=int),
            "category_id": request.form.get("category_id", type=int),
        }
        films.update(film_id=film_id, data=payload)
        flash("Film updated", "success")
        return redirect(url_for("film_detail", film_id=film_id))

    film = films.get(film_id)
    categories = films.categories()
    film_actors = films.actors(film_id)
    available_actors = films.available_actors(film_id)
    languages = films.languages()

    return render_template("film_detail.html",
                           film=film,
                           categories=categories,
                           actors=film_actors,
                           available_actors=available_actors,
                           languages=languages)

@app.route("/films/add", methods=["GET", "POST"])
def add_film():
    if request.method == "POST":
        title = request.form.get("title")
        description = request.form.get("description")
        release_year = request.form.get("release_year")
        language_id = request.form.get("language_id")
        rental_duration = request.form.get("rental_duration")
        rental_rate = request.form.get("rental_rate")
        length = request.form.get("length")
        replacement_cost = request.form.get("replacement_cost")
        rating = request.form.get("rating")
        category_id = request.form.get("category_id")

        if not all([title, description, release_year, language_id, rental_duration, 
                    rental_rate, length, replacement_cost, rating, category_id]):
            flash("All fields are required. Please fill in all data.", "danger")
            languages = films.languages()
            categories = films.categories()
            return render_template("film_add.html", languages=languages, categories=categories, 
                                   form_data=request.form)

        payload = {
            "title": title,
            "description": description,
            "release_year": int(release_year),
            "language_id": int(language_id),
            "rental_duration": int(rental_duration),
            "rental_rate": float(rental_rate),
            "length": int(length),
            "replacement_cost": float(replacement_cost),
            "rating": rating,
            "category_id": int(category_id)
        }

        try:
            new_id = films.add(payload)
            flash(f"Film '{title}' created successfully!", "success")
            return redirect(url_for("film_detail", film_id=new_id))
        except Exception as e:
            flash(f"Error creating film: {e}", "danger")

    languages = films.languages()
    categories = films.categories()
    return render_template("film_add.html", languages=languages, categories=categories)

@app.post("/film/<int:film_id>/delete")
def delete_film(film_id):
    try:
        films.delete(film_id)
        flash("Film deleted successfully", "success")
        return redirect(url_for("films_list"))
    except Exception as e:
        flash(f"Could not delete film. Error: {e}", "danger")
        return redirect(url_for("film_detail", film_id=film_id))

@app.post("/film/<int:film_id>/actors/add")
def add_actor(film_id):
    actor_id = request.form.get("actor_id", type=int)
    if not actor_id:
        flash("Select an actor to add", "warning")
        return redirect(url_for("film_detail", film_id=film_id))
    films.add_actor(film_id=film_id, actor_id=actor_id)
    flash("Actor added", "success")
    return redirect(url_for("film_detail", film_id=film_id))

@app.post("/film/<int:film_id>/actors/<int:actor_id>/remove")
def remove_actor(film_id, actor_id):
    films.remove_actor(film_id=film_id, actor_id=actor_id)
    flash("Actor removed", "info")
    return redirect(url_for("film_detail", film_id=film_id))

# --- ADDRESS ---
@app.route("/address")
def address():
    address_text = request.args.get("address", default=None, type=str)
    district = request.args.get("district", default=None, type=str)
    postal_code = request.args.get("postal_code", default=None, type=str)
    phone = request.args.get("phone", default=None, type=str)
    city_id = request.args.get("city_id", type=int)
    country_id = request.args.get("country_id", type=int)
    page = max(request.args.get("page", default=1, type=int), 1)
    page_size = 20

    rows = addresses.search(
        address=address_text, district=district, postal_code=postal_code,
        phone=phone, city_id=city_id, country_id=country_id,
        page=page, page_size=page_size
    )
    
    total_count = addresses.count_search(
        address=address_text, district=district, postal_code=postal_code,
        phone=phone, city_id=city_id, country_id=country_id
    )
    total_pages = math.ceil(total_count / page_size)
    
    cities = addresses.get_cities()
    countries = addresses.get_countries()

    return render_template("address.html",
                           addresses=rows, cities=cities, countries=countries,
                           sel_city_id=city_id, sel_country_id=country_id,
                           address=address_text, district=district,
                           postal_code=postal_code, phone=phone, page=page,
                           total_pages=total_pages)

@app.route("/address/top-countries")
def address_top_countries():
    rows = addresses.top_countries_by_customers()
    return render_template("address_top_countries.html", rows=rows)

@app.route("/address/<int:address_id>", methods=["GET", "POST"])
def address_detail(address_id):
    if request.method == "POST":
        payload = {
            "address": request.form.get("address"),
            "address2": request.form.get("address2"),
            "district": request.form.get("district"),
            "city_id": request.form.get("city_id", type=int),
            "postal_code": request.form.get("postal_code"),
            "phone": request.form.get("phone"),
        }
        addresses.update(address_id=address_id, data=payload)
        flash("Address updated", "success")
        return redirect(url_for("address_detail", address_id=address_id))

    addr = addresses.get(address_id)
    cities = addresses.get_cities()
    
    if not addr:
        flash("Address not found", "danger")
        return redirect(url_for("address"))

    return render_template("address_detail.html",
                           address=addr,
                           cities=cities)

@app.post("/address/<int:address_id>/delete")
def address_delete(address_id):
    try:
        addresses.delete(address_id)
        flash("Address deleted successfully", "success")
    except Exception as e:
        flash(f"Error: {str(e)}", "danger")
    return redirect(url_for("address"))

@app.route("/address/add", methods=["GET", "POST"])
def address_add():
    """Add a new address"""
    if request.method == "POST":
        payload = {
            "address": request.form.get("address"),
            "address2": request.form.get("address2"),
            "district": request.form.get("district"),
            "city_id": request.form.get("city_id", type=int),
            "postal_code": request.form.get("postal_code"),
            "phone": request.form.get("phone"),
        }
        try:
            addresses.add(payload)
            flash("Address added successfully", "success")
            return redirect(url_for("address"))
        except Exception as e:
            flash(f"Error: {str(e)}", "danger")
    
    cities = addresses.get_cities()
    return render_template("address_add.html", cities=cities)

# --- CUSTOMERS  ---
@app.route("/customers")
@app.route("/customers")
def customers_list():
    q = request.args.get("q", type=str)
    page = max(request.args.get("page", default=1, type=int), 1)
    page_size = 20

    rows = customers.list_customers(q=q, page=page, page_size=page_size)
    total_count = customers.count_search(q=q)
    total_pages = math.ceil(total_count / page_size)

    return render_template("customers.html",
                           customers=rows, q=q, page=page,
                           total_pages=total_pages)


@app.route("/customer/add", methods=["GET", "POST"])
def customer_add():
    if request.method == "POST":
        payload = {
            "first_name": request.form.get("first_name"),
            "last_name": request.form.get("last_name"),
            "email": request.form.get("email"),
            "address_id": request.form.get("address_id", type=int),
            "active": 1 if request.form.get("active") else 0
        }
        try:
            customers.add(payload)
            flash("Customer saved successfully!", "success")
            return redirect(url_for("customers_list"))
        except Exception as e:
            flash(f"Error adding customer: {e}", "danger")

    all_addresses = addresses.search(page_size=100)
    return render_template("customer_detail.html", customer=None, addresses=all_addresses)

@app.route("/customer/<int:customer_id>", methods=["GET", "POST"])
def customer_detail(customer_id):
    if request.method == "POST":
        payload = {
            "first_name": request.form.get("first_name"),
            "last_name": request.form.get("last_name"),
            "email": request.form.get("email"),
            "address_id": request.form.get("address_id", type=int),
            "active": 1 if request.form.get("active") else 0
        }
        try:
            customers.update(customer_id, payload)
            flash("Customer updated successfully", "success")
            return redirect(url_for("customers_list"))
        except Exception as e:
            flash(f"Error updating: {e}", "danger")
            return redirect(url_for("customer_detail", customer_id=customer_id))
    
    cust = customers.get(customer_id)
    if not cust:
        flash("Customer not found", "danger")
        return redirect(url_for("customers_list"))
        
    all_addresses = addresses.search(page_size=100)
    return render_template("customer_detail.html", customer=cust, addresses=all_addresses)

@app.post("/customer/<int:customer_id>/delete")
def customer_delete(customer_id):
    try:
        customers.delete(customer_id)
        flash("Customer deleted", "info")
    except Exception as e:
        flash(f"Cannot delete customer (Has rentals/payments?): {e}", "danger")
    return redirect(url_for("customers_list"))

@app.route("/customers/top")
def customers_top():
    rows = customers.top_customers_by_payment()
    return render_template("customers_top.html", customers=rows)
@app.route("/customers/top-spenders")
def customers_top_spenders():
    limit = request.args.get("limit", default=20, type=int)
    rows = customers.top_spenders(limit=limit)
    return render_template("customers_top_spenders.html", rows=rows, limit=limit)

# --- PAYMENTS ---
@app.route("/payments")
def payments_list():
    # 1. Get query parameters from the URL
    q = request.args.get("q", type=str)
    payment_method = request.args.get("payment_method", type=str)
    sort_order = request.args.get("sort_order", default="desc", type=str)
    
    # Get the current page number (default is 1). Ensure it's at least 1.
    page = max(request.args.get("page", default=1, type=int), 1)
    
    # --- DEĞİŞİKLİK BURADA YAPILDI ---
    # Define how many items to show per page (Updated to 20 to match friends' projects)
    per_page = 20 

    # 2. Call the search function
    rows, total_count = payments.search(
        q=q, 
        payment_method=payment_method, 
        sort_order=sort_order, 
        page=page, 
        per_page=per_page
    )
    
    # 3. Calculate Total Pages
    total_pages = math.ceil(total_count / per_page)
    if total_pages == 0:
        total_pages = 1

    # 4. Render the template
    return render_template(
        "payment.html", 
        payments=rows, 
        q=q, 
        sel_method=payment_method, 
        sel_sort=sort_order, 
        page=page, 
        total_pages=total_pages
    )
    
@app.route('/payments/edit/<int:payment_id>', methods=['GET', 'POST'])
def edit_payment(payment_id):
    # --- POST REQUEST ---
    if request.method == 'POST':
        try:
            payments.update_payment(payment_id, request.form)
            return redirect(url_for('payments_list')) 
        except Exception as e:
            return f"An error occurred: {e}"

    # --- GET REQUEST ---
    try:
        # We only unpack 2 values now (payment and customers)
        payment, customers = payments.get_payment_details(payment_id)
        
        if not payment:
            return "Payment not found", 404

        # Render without staff_members
        return render_template(
            'payments_edit.html', 
            payment=payment, 
            customers=customers
        )
    except Exception as e:
        return f"Error fetching data: {e}"

@app.route('/payments/delete/<int:payment_id>')
def delete_payment_route(payment_id):
    try:
        payments.delete_payment(payment_id)
        
        return redirect(url_for('payments_list'))
    except Exception as e:
        return f"Error deleting payment: {e}"

@app.route('/payments/add', methods=['GET', 'POST'])
def add_payment():
    # --- POST REQUEST (Save Button Clicked) ---
    if request.method == 'POST':
        try:
            payments.add_payment(request.form)
            return redirect(url_for('payments_list'))
        except Exception as e:
            return f"Error adding payment: {e}"

    # --- GET REQUEST (Show Form) ---
    try:
        # We need the customer list for the dropdown menu
        customers = payments.get_all_customers()
        return render_template('payments_add.html', customers=customers)
    except Exception as e:
        return f"Error loading page: {e}"
        
# --- RENTALS ---
@app.route("/rentals")
def rentals_list():
    q = request.args.get("q", type=str)
    status = request.args.get("status", type=str)
    page = max(request.args.get("page", default=1, type=int), 1)
    
    rows = rentals.search(q=q, status=status, page=page)
    
    return render_template("rentals.html", rentals=rows, q=q, sel_status=status, page=page)

@app.route("/rental/add", methods=["GET", "POST"])
def rental_add():
    if request.method == "POST":
        customer_id = request.form.get("customer_id", type=int)
        film_id = request.form.get("film_id", type=int)
        
        if customer_id and film_id:
            try:
                rentals.add(customer_id=customer_id, film_id=film_id)
                flash("Rental created successfully", "success")
                return redirect(url_for("rentals_list"))
            except ValueError as e:
                flash(str(e), "danger")
            except Exception as e:
                flash(f"Error: {e}", "danger")
        else:
            flash("Please select both customer and film", "warning")

    all_customers = customers.list_customers(page_size=500) 
    all_films = films.search(page_size=500)
    
    return render_template("rental_add.html", customers=all_customers, films=all_films)

@app.route("/rental/<int:rental_id>/return", methods=["POST"])
def rental_return(rental_id):
    rentals.return_film(rental_id)
    flash("Movie returned successfully", "success")
    return redirect(url_for("rentals_list"))

@app.get("/health")
def health():
    try:
        n = films.count()
        return f"OK. films={n}"
    except Exception as e:
        return f"DB error: {e}", 500

if __name__ == "__main__":

    app.run(debug=True)


