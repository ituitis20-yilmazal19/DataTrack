from typing import Callable, Dict, List, Any
import mysql.connector

def _dict_rows(cur) -> List[Dict[str, Any]]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

class Films:
    """Data-access helpers for the Sakila-like schema using mysql.connector."""
    def __init__(self, connection_factory: Callable[[], mysql.connector.MySQLConnection]):
        self.connection_factory = connection_factory

    def search(self, category_id=None, language_id=None, q=None, page=1, page_size=20):
        offset = (page - 1) * page_size
        where = []
        params = []

        if language_id:
            where.append("f.language_id = %s")
            params.append(language_id)
        if category_id:
            where.append("fc.category_id = %s")
            params.append(category_id)
        if q:
            where.append("f.title LIKE %s")
            params.append(f"%{q}%")

        where_clause = ("WHERE " + " AND ".join(where)) if where else ""

        sql = f"""
            SELECT
                f.film_id, f.title, f.release_year, f.rating,
                l.name AS language_name,
                GROUP_CONCAT(DISTINCT c.name ORDER BY c.name SEPARATOR ', ') AS categories
            FROM film AS f
            JOIN language l ON l.language_id = f.language_id
            LEFT JOIN film_category fc ON fc.film_id = f.film_id
            LEFT JOIN category c ON c.category_id = fc.category_id
            {where_clause}
            GROUP BY f.film_id, f.title, f.release_year, f.rating, l.name
            ORDER BY f.title
            LIMIT %s OFFSET %s
        """
        params += [page_size, offset]
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, params)
            return _dict_rows(cur)

    def get(self, film_id: int):
        sql = """
            SELECT f.*, l.name AS language_name, ol.name AS original_language_name
            FROM film f
            JOIN language l ON l.language_id = f.language_id
            LEFT JOIN language ol ON ol.language_id = f.original_language_id
            WHERE f.film_id = %s
        """
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, (film_id,))
            rows = _dict_rows(cur)
            return rows[0] if rows else None

    def film_categories(self, film_id: int):
        sql = """
            SELECT c.category_id, c.name
            FROM category c
            JOIN film_category fc ON fc.category_id = c.category_id
            WHERE fc.film_id = %s
            ORDER BY c.name
        """
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, (film_id,))
            return _dict_rows(cur)

    def actors(self, film_id: int):
        sql = """
            SELECT a.actor_id, a.first_name, a.last_name
            FROM actor a
            JOIN film_actor fa ON fa.actor_id = a.actor_id
            WHERE fa.film_id = %s
            ORDER BY a.last_name, a.first_name
        """
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, (film_id,))
            return _dict_rows(cur)

    def available_actors(self, film_id: int):
        sql = """
            SELECT a.actor_id, a.first_name, a.last_name
            FROM actor a
            WHERE NOT EXISTS (
                SELECT 1 FROM film_actor fa
                WHERE fa.film_id = %s AND fa.actor_id = a.actor_id
            )
            ORDER BY a.last_name, a.first_name
        """
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, (film_id,))
            return _dict_rows(cur)

    def languages(self):
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute("SELECT language_id, name FROM language ORDER BY name")
            return _dict_rows(cur)

    def categories(self):
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute("SELECT category_id, name FROM category ORDER BY name")
            return _dict_rows(cur)

    def update(self, film_id: int, data: Dict[str, Any]):
        sql = """
            UPDATE film
               SET title=%s,
                   description=%s,
                   release_year=%s,
                   language_id=%s,
                   rating=%s,
                   rental_rate=%s,
                   `length`=%s,
                   replacement_cost=%s,
                   rental_duration=%s
             WHERE film_id=%s
        """
        params = (
            data.get("title"),
            data.get("description"),
            data.get("release_year"),
            data.get("language_id"),
            data.get("rating"),
            data.get("rental_rate"),
            data.get("length"),
            data.get("replacement_cost"),
            data.get("rental_duration"),
            film_id,
        )
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, params)

    def add_actor(self, film_id: int, actor_id: int):
        sql_check = "SELECT 1 FROM film_actor WHERE film_id=%s AND actor_id=%s"
        sql_ins = "INSERT INTO film_actor(actor_id, film_id) VALUES(%s, %s)"
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql_check, (film_id, actor_id))
            if cur.fetchone() is None:
                cur.execute(sql_ins, (actor_id, film_id))

    def remove_actor(self, film_id: int, actor_id: int):
        sql = "DELETE FROM film_actor WHERE film_id=%s AND actor_id=%s"
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, (film_id, actor_id))

    def count(self) -> int:
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM film")
            (n,) = cur.fetchone()
            return int(n)

class Customers:
    """Data-access helpers for the customer table and related analytics."""

    def __init__(self, connection_factory: Callable[[], mysql.connector.MySQLConnection]):
        self.connection_factory = connection_factory

    def list_customers(self, city: str | None = None, active: bool | None = None, limit: int = 100):
        """
        List customers with address, city and country info.
        .
        """
        query = """
            SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                c.email,
                c.active,
                c.create_date,
                a.address,
                a.district,
                a.postal_code,
                a.phone,
                ci.city,
                co.country
            FROM customer c
            JOIN address a ON c.address_id = a.address_id
            JOIN city ci ON a.city_id = ci.city_id
            JOIN country co ON ci.country_id = co.country_id
        """

        filters = []
        params = []

        if city:
            filters.append("ci.city = %s")
            params.append(city)
        if active is not None:
            filters.append("c.active = %s")
            params.append(1 if active else 0)

        if filters:
            query += " WHERE " + " AND ".join(filters)

        query += " ORDER BY c.customer_id ASC LIMIT %s"
        params.append(limit)

        with self.connection_factory() as conn, conn.cursor(dictionary=True) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def search_customers(self, q: str, limit: int = 50):
        """
        Search customers by first name, last name or email.
        """
        like = f"%{q}%"
        query = """
            SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                c.email,
                c.active
            FROM customer c
            WHERE c.first_name LIKE %s
               OR c.last_name LIKE %s
               OR c.email LIKE %s
            ORDER BY c.last_name, c.first_name
            LIMIT %s
        """
        params = (like, like, like, limit)
        with self.connection_factory() as conn, conn.cursor(dictionary=True) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def top_customers_by_payment(self, limit: int = 10):
        """
            Return customers ordered by total payment amount (descending).
        """
        query = """
            SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                c.email,
                SUM(p.amount) AS total_spent,
                COUNT(p.payment_id) AS payment_count
            FROM customer c
            JOIN payment p ON p.customer_id = c.customer_id
            GROUP BY c.customer_id, c.first_name, c.last_name, c.email
            ORDER BY total_spent DESC
            LIMIT %s
        """
        with self.connection_factory() as conn, conn.cursor(dictionary=True) as cur:
            cur.execute(query, (limit,))
            return cur.fetchall()

    def get_customer(self, customer_id: int):
        """
        Get a single customer with address details.
        """
        query = """
            SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                c.email,
                c.active,
                c.create_date,
                a.address,
                a.district,
                a.postal_code,
                a.phone,
                ci.city,
                co.country
            FROM customer c
            JOIN address a ON c.address_id = a.address_id
            JOIN city ci ON a.city_id = ci.city_id
            JOIN country co ON ci.country_id = co.country_id
            WHERE c.customer_id = %s
        """
        with self.connection_factory() as conn, conn.cursor(dictionary=True) as cur:
            cur.execute(query, (customer_id,))
            row = cur.fetchone()
            return row


class Addresses:
    """Data-access helpers for the address table."""
    def __init__(self, connection_factory: Callable[[], mysql.connector.MySQLConnection]):
        self.connection_factory = connection_factory

    def search(self, address=None, district=None, postal_code=None, phone=None, 
               city_id=None, country_id=None, page=1, page_size=20):
        """Search addresses with optional filters"""
        offset = (page - 1) * page_size
        where = []
        params = []

        if address:
            where.append("a.address LIKE %s")
            params.append(f"%{address}%")
        if district:
            where.append("a.district LIKE %s")
            params.append(f"%{district}%")
        if postal_code:
            where.append("a.postal_code LIKE %s")
            params.append(f"%{postal_code}%")
        if phone:
            where.append("a.phone LIKE %s")
            params.append(f"%{phone}%")
        if city_id:
            where.append("a.city_id = %s")
            params.append(city_id)
        if country_id:
            where.append("co.country_id = %s")
            params.append(country_id)

        where_clause = ("WHERE " + " AND ".join(where)) if where else ""

        sql = f"""
            SELECT
                a.address_id, a.address, a.address2, a.district,
                a.postal_code, a.phone,
                c.city_id, c.city,
                co.country_id, co.country
            FROM address a
            JOIN city c ON a.city_id = c.city_id
            JOIN country co ON c.country_id = co.country_id
            {where_clause}
            ORDER BY a.address_id ASC
            LIMIT %s OFFSET %s
        """
        params += [page_size, offset]
        
        with self.connection_factory() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def get(self, address_id: int):
        """Get a single address by ID"""
        sql = """
            SELECT
                a.address_id, a.address, a.address2, a.district,
                a.postal_code, a.phone,
                c.city_id, c.city,
                co.country_id, co.country
            FROM address a
            JOIN city c ON a.city_id = c.city_id
            JOIN country co ON c.country_id = co.country_id
            WHERE a.address_id = %s
        """
        with self.connection_factory() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, (address_id,))
            row = cur.fetchone()
            return row

    def update(self, address_id: int, data: Dict[str, Any]):
        """Update an address"""
        sql = """
            UPDATE address
            SET address=%s, address2=%s, district=%s, city_id=%s,
                postal_code=%s, phone=%s
            WHERE address_id=%s
        """
        params = (
            data.get("address"),
            data.get("address2"),
            data.get("district"),
            data.get("city_id", type=int),
            data.get("postal_code"),
            data.get("phone"),
            address_id,
        )
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, params)

    def add(self, data: Dict[str, Any]):
        """Add a new address"""
        sql = """
            INSERT INTO address (address, address2, district, city_id, postal_code, phone)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            data.get("address"),
            data.get("address2"),
            data.get("district"),
            data.get("city_id"),
            data.get("postal_code"),
            data.get("phone"),
        )
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, params)

    def delete(self, address_id: int):
        """Delete an address"""
        sql = "DELETE FROM address WHERE address_id=%s"
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, (address_id,))

    def get_cities(self, city_id=None, city_name=None, country_name=None, country_id=None):
        """Get cities with optional filters"""
        sql = """
            SELECT c.city_id, c.city, co.country_id, co.country
            FROM city c
            JOIN country co ON c.country_id = co.country_id
        """
        where = []
        params = []
        
        if city_id:
            where.append("c.city_id = %s")
            params.append(city_id)
        if city_name:
            where.append("c.city LIKE %s")
            params.append(f"%{city_name}%")
        if country_name:
            where.append("co.country LIKE %s")
            params.append(f"%{country_name}%")
        if country_id:
            where.append("co.country_id = %s")
            params.append(country_id)
        
        if where:
            sql += " WHERE " + " AND ".join(where)
        
        sql += " ORDER BY c.city_id ASC"
        
        with self.connection_factory() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def get_countries(self, country_id=None, name=None):
        """Get countries with optional filters"""
        sql = "SELECT country_id, country FROM country"
        where = []
        params = []
        
        if country_id:
            where.append("country_id = %s")
            params.append(country_id)
        if name:
            where.append("country LIKE %s")
            params.append(f"%{name}%")
        
        if where:
            sql += " WHERE " + " AND ".join(where)
        
        sql += " ORDER BY country_id ASC"
        
        with self.connection_factory() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, params)

            return cur.fetchall()

class Payments:
    """Data-access helpers for the payment table."""

    def __init__(self, connection_factory: Callable[[], mysql.connector.MySQLConnection]):
        self.connection_factory = connection_factory

    def search(self, q=None, payment_method=None, sort_order="desc", page=1, page_size=20):
        """
        Search payments with filters (q for amount/ID, payment_method) and pagination.
        """
        offset = (page - 1) * page_size
        where = []
        params = []

        if payment_method:
            where.append("Payment_method = %s")
            params.append(payment_method)

        if q:
            try:
                clean_q = q.replace(',', '.')
                val = float(clean_q)
                
                where.append("(amount = %s OR customer_id = %s OR payment_id = %s)")
                params.extend([val, int(val), int(val)])
            except ValueError:
                pass

        where_clause = ("WHERE " + " AND ".join(where)) if where else ""

        if sort_order == "asc":
            order_clause = "ORDER BY payment_date ASC"
        else:
            order_clause = "ORDER BY payment_date DESC"

        sql = f"""
            SELECT 
                payment_id, customer_id, staff_id, rental_id, 
                amount, payment_date, last_update, Payment_method
            FROM payment
            {where_clause}
            {order_clause}
            LIMIT %s OFFSET %s
        """
        params.extend([page_size, offset])

        with self.connection_factory() as conn, conn.cursor(dictionary=True) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def get(self, payment_id: int):
        """Get a single payment detail."""
        sql = "SELECT * FROM payment WHERE payment_id = %s"
        with self.connection_factory() as conn, conn.cursor(dictionary=True) as cur:
            cur.execute(sql, (payment_id,))
            row = cur.fetchone()
            return row

