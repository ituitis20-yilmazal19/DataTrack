from flask import Flask, render_template, request, redirect, url_for, flash
from settings import db_user, db_password, db_host, db_name
import pymysql
from utils.table_operations import Films

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
    return render_template("placeholder.html", title="Address")

@app.route("/customers")
def customers():
    return render_template("placeholder.html", title="Customers")

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
