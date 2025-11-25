from flask import Flask, render_template, request, redirect, url_for, flash
from settings import db_user, db_password, db_host, db_name
import pymysql
from utils.table_operations import Films, Customers, Addresses

app = Flask(__name__)
app.secret_key = "dev-only-change-me"

def get_connection():
    return pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        charset="utf8mb4",
        autocommit=True,
    )

films = Films(connection_factory=get_connection)
customers = Customers(connection_factory=get_connection)
addresses = Addresses(connection_factory=get_connection)

@app.route("/")
def main():
    return render_template("main.html")

@app.route("/films")
def films_list():
    category_id = request.args.get("category_id", type=int)
    language_id = request.args.get("language_id", type=int)
    q = request.args.get("q", type=str)
    page = max(request.args.get("page", default=1, type=int), 1)
    page_size = 20

    rows = films.search(category_id=category_id, language_id=language_id, q=q,
                        page=page, page_size=page_size)
    languages = films.languages()
    categories = films.categories()

    return render_template("films.html",
                           films=rows,
                           languages=languages,
                           categories=categories,
                           sel_category_id=category_id,
                           sel_language_id=language_id,
                           q=q, page=page)

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
        }
        films.update(film_id=film_id, data=payload)
        flash("Film updated", "success")
        return redirect(url_for("film_detail", film_id=film_id))

    film = films.get(film_id)
    film_categories = films.film_categories(film_id)
    film_actors = films.actors(film_id)
    available_actors = films.available_actors(film_id)
    languages = films.languages()

    return render_template("film_detail.html",
                           film=film,
                           film_categories=film_categories,
                           actors=film_actors,
                           available_actors=available_actors,
                           languages=languages)

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

@app.route("/address")
def address():
    """List addresses with optional filters"""
    address_text = request.args.get("address", default=None, type=str)
    district = request.args.get("district", default=None, type=str)
    postal_code = request.args.get("postal_code", default=None, type=str)
    phone = request.args.get("phone", default=None, type=str)
    city_id = request.args.get("city_id", type=int)
    country_id = request.args.get("country_id", type=int)
    page = max(request.args.get("page", default=1, type=int), 1)
    page_size = 20

    rows = addresses.search(
        address=address_text,
        district=district,
        postal_code=postal_code,
        phone=phone,
        city_id=city_id,
        country_id=country_id,
        page=page,
        page_size=page_size
    )
    
    cities = addresses.get_cities()
    countries = addresses.get_countries()

    return render_template("address.html",
                           addresses=rows,
                           cities=cities,
                           countries=countries,
                           sel_city_id=city_id,
                           sel_country_id=country_id,
                           address=address_text,
                           district=district,
                           postal_code=postal_code,
                           phone=phone,
                           page=page)

@app.route("/address/<int:address_id>", methods=["GET", "POST"])
def address_detail(address_id):
    """View and edit address details"""
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

    return render_template("placeholder.html",
                           address=addr,
                           cities=cities)

@app.post("/address/<int:address_id>/delete")
def address_delete(address_id):
    """Delete an address"""
    try:
        addresses.delete(address_id)
        flash("Address deleted successfully", "success")
    except Exception as e:
        flash(f"Error: {str(e)}", "danger")
    return redirect(url_for("address"))



@app.route("/customers")
def customers_list():
    """Show a simple list of customers."""
    rows = customers.list_customers()  # default limit içeriden geliyor, parametre yok
    return render_template("customers.html", customers=rows)


@app.route("/customers/search")
def customers_search():
    """Search customers by name or email."""
    q = request.args.get("q", default="", type=str)
    rows = []
    if q:
        rows = customers.search_customers(q)
    return render_template("customers_search.html", customers=rows, query=q)


@app.route("/customers/top")
def customers_top():
    """Show customers ordered by total payment amount."""
    rows = customers.top_customers_by_payment()
    return render_template("customers_top.html", customers=rows)
@app.route("/payments")
def payments():
    return render_template("placeholder.html", title="Payments")

@app.route("/rentals")
def rentals():
    return render_template("placeholder.html", title="Rentals")


# Quick health check
@app.get("/health")
def health():
    try:
        n = films.count()
        return f"OK. films={n}"
    except Exception as e:
        return f"DB error: {e}", 500

if __name__ == "__main__":
    app.run(debug=True)
