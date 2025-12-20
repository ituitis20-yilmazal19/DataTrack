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
            SELECT f.*, l.name AS language_name, ol.name AS original_language_name,
                   fc.category_id
            FROM film f
            JOIN language l ON l.language_id = f.language_id
            LEFT JOIN language ol ON ol.language_id = f.original_language_id
            LEFT JOIN film_category fc ON fc.film_id = f.film_id
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

    def add(self, data: Dict[str, Any]) -> int:
        sql_film = """
            INSERT INTO film (
                title, description, release_year, language_id, 
                rental_duration, rental_rate, length, replacement_cost, rating
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            data["title"], 
            data["description"], 
            data["release_year"],
            data["language_id"], 
            data["rental_duration"], 
            data["rental_rate"],
            data["length"], 
            data["replacement_cost"], 
            data["rating"]
        )
        
        category_id = data.get("category_id")

        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql_film, params)
            new_film_id = cur.lastrowid
            
            if category_id:
                sql_cat = "INSERT INTO film_category (film_id, category_id) VALUES (%s, %s)"
                cur.execute(sql_cat, (new_film_id, category_id))
                
            return new_film_id

    def delete(self, film_id: int):
        """
        First removes dependencies in film_actor and film_category 
        to prevent Foreign Key constraints from failing.
        """
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute("DELETE FROM film_actor WHERE film_id = %s", (film_id,))
            cur.execute("DELETE FROM film_category WHERE film_id = %s", (film_id,))
            cur.execute("DELETE FROM film WHERE film_id = %s", (film_id,))
    
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
        sql_film = """
            UPDATE film
               SET title=%s, description=%s, release_year=%s, language_id=%s,
                   rating=%s, rental_rate=%s, `length`=%s, replacement_cost=%s,
                   rental_duration=%s
             WHERE film_id=%s
        """
        params_film = (
            data.get("title"), data.get("description"), data.get("release_year"),
            data.get("language_id"), data.get("rating"), data.get("rental_rate"),
            data.get("length"), data.get("replacement_cost"),
            data.get("rental_duration"),
            film_id,
        )
        category_id = data.get("category_id")
        
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql_film, params_film)
            
            if category_id:
                update_cat_sql = "UPDATE film_category SET category_id=%s WHERE film_id=%s"
                cur.execute(update_cat_sql, (category_id, film_id))
                
                if cur.rowcount == 0:
                    insert_cat_sql = "INSERT INTO film_category (film_id, category_id) VALUES (%s, %s)"
                    cur.execute(insert_cat_sql, (film_id, category_id))

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

    def count_search(self, category_id=None, language_id=None, q=None):
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
            SELECT COUNT(DISTINCT f.film_id)
            FROM film AS f
            JOIN language l ON l.language_id = f.language_id
            LEFT JOIN film_category fc ON fc.film_id = f.film_id
            LEFT JOIN category c ON c.category_id = fc.category_id
            {where_clause}
        """

        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]

class Customers:
    """Data-access helpers for the customer table and related analytics."""

    def __init__(self, connection_factory: Callable[[], mysql.connector.MySQLConnection]):
        self.connection_factory = connection_factory

    def list_customers(self, q: str = None, page: int = 1, page_size: int = 20):
        """
        List customers with pagination and search (q).
        Includes join with address/city/country for display.
        """
        offset = (page - 1) * page_size
        where = []
        params = []

        if q:
            like_q = f"%{q}%"
            where.append("(c.first_name LIKE %s OR c.last_name LIKE %s OR c.email LIKE %s)")
            params.extend([like_q, like_q, like_q])

        where_clause = ("WHERE " + " AND ".join(where)) if where else ""

        query = f"""
            SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                c.email,
                c.active,
                c.create_date,
                a.address,
                ci.city,
                co.country
            FROM customer c
            LEFT JOIN address a ON c.address_id = a.address_id
            LEFT JOIN city ci ON a.city_id = ci.city_id
            LEFT JOIN country co ON ci.country_id = co.country_id
            {where_clause}
            ORDER BY c.last_name, c.first_name
            LIMIT %s OFFSET %s
        """
        params.extend([page_size, offset])

        with self.connection_factory() as conn, conn.cursor(dictionary=True) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def get(self, customer_id: int):
        """
        Get a single customer with details for editing.
        """
        query = """
            SELECT 
                c.*,
                a.address, a.city_id, ci.city, co.country
            FROM customer c
            LEFT JOIN address a ON c.address_id = a.address_id
            LEFT JOIN city ci ON a.city_id = ci.city_id
            LEFT JOIN country co ON ci.country_id = co.country_id
            WHERE c.customer_id = %s
        """
        with self.connection_factory() as conn, conn.cursor(dictionary=True) as cur:
            cur.execute(query, (customer_id,))
            return cur.fetchone()

    def add(self, data: Dict[str, Any]):
        """Create a new customer."""
        sql = """
            INSERT INTO customer (first_name, last_name, email, address_id, active, create_date)
            VALUES (%s, %s, %s, %s, %s, NOW())
        """
        params = (
            data.get("first_name"),
            data.get("last_name"),
            data.get("email"),
            data.get("address_id"),
            data.get("active", 1)
        )
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(sql, params)

    def update(self, customer_id: int, data: Dict[str, Any]):
        """Update existing customer."""
        sql = """
            UPDATE customer 
            SET first_name=%s, last_name=%s, email=%s, address_id=%s, active=%s
            WHERE customer_id=%s
        """
        params = (
            data.get("first_name"),
            data.get("last_name"),
            data.get("email"),
            data.get("address_id"),
            data.get("active"),
            customer_id
        )
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(sql, params)

    def delete(self, customer_id: int):
        """Delete a customer."""
        # Note: If foreign keys (rentals/payments) exist without CASCADE, this might fail.
        sql = "DELETE FROM customer WHERE customer_id = %s"
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(sql, (customer_id,))

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
            data.get("city_id"),
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

    def count_search(self, address=None, district=None, postal_code=None, phone=None, 
                     city_id=None, country_id=None):
        """Count addresses matching search criteria"""
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
            SELECT COUNT(DISTINCT a.address_id)
            FROM address a
            JOIN city c ON a.city_id = c.city_id
            JOIN country co ON c.country_id = co.country_id
            {where_clause}
        """

        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]

    def top_countries_by_customers(self, limit: int = 20):
        """
        Top countries by customer count.
        Returns: rank, customer_count, country
        """
        sql = """
            SELECT 
                COUNT(*) AS customer_count, 
                co.country
            FROM address a
            JOIN city c ON a.city_id = c.city_id
            JOIN country co ON c.country_id = co.country_id
            JOIN customer cus ON a.address_id = cus.address_id
            GROUP BY co.country
            ORDER BY customer_count DESC
            LIMIT %s
        """
        params = [limit]
        
        with self.connection_factory() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, params)
            results = cur.fetchall()
            # Python'da rank ekle
            for idx, row in enumerate(results, start=1):
                row['rank'] = idx
            return results

class Payments:
    """Data-access helpers for the payment table."""

    def __init__(self, connection_factory: Callable[[], mysql.connector.MySQLConnection]):
        self.connection_factory = connection_factory

    def search(self, q=None, payment_method=None, sort_order="desc", page=1, per_page=10):
        """
        Searches and filters payments based on query, method, and sorting.
        Now performs a JOIN with the customer table to fetch names.
        """
        offset = (page - 1) * per_page
        params = []
        conditions = []

        # SQL Query: We join 'payment' with 'customer' to access first_name and last_name.
        sql = """
            SELECT 
                p.payment_id, p.customer_id, p.rental_id, 
                p.amount, p.payment_date, p.last_update, p.payment_method,
                c.first_name, c.last_name
            FROM payment p
            JOIN customer c ON p.customer_id = c.customer_id
            WHERE 1=1
        """

        # Search Logic: Filter by ID, Amount, First Name, or Last Name
        if q:
            conditions.append("""
                (CAST(p.payment_id AS CHAR) LIKE %s OR 
                 CAST(p.amount AS CHAR) LIKE %s OR 
                 c.first_name LIKE %s OR 
                 c.last_name LIKE %s)
            """)
            term = f"%{q}%"
            # We pass the search term 4 times for the 4 conditions above
            params.extend([term, term, term, term])

        # Filter by Payment Method (e.g., 'Credit Card', 'PayPal')
        if payment_method:
            conditions.append("p.payment_method = %s")
            params.append(payment_method)

        # Append conditions to the main SQL query
        if conditions:
            sql += " AND " + " AND ".join(conditions)

        # Sorting Logic: Sort by payment_date
        order_dir = "ASC" if sort_order == "asc" else "DESC"
        sql += f" ORDER BY p.payment_date {order_dir}"

        # Pagination: Limit the number of results per page
        sql += " LIMIT %s OFFSET %s"
        params.extend([per_page, offset])

        # Execute the query
        with self.connection_factory() as cn, cn.cursor(dictionary=True) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        
        return rows

    def get(self, payment_id: int):
        """Get a single payment detail."""
        sql = "SELECT * FROM payment WHERE payment_id = %s"
        with self.connection_factory() as conn, conn.cursor(dictionary=True) as cur:
            cur.execute(sql, (payment_id,))
            row = cur.fetchone()
            return row

    def get_payment_details(self, payment_id):
        """
        Fetches payment and customers. 
        REMOVED: Staff fetching (since table doesn't exist).
        """
        with self.connection_factory() as cn, cn.cursor(dictionary=True) as cur:
            # 1. Fetch the specific payment record
            cur.execute("SELECT * FROM payment WHERE payment_id = %s", (payment_id,))
            payment = cur.fetchone()

            # 2. Fetch all customers for the dropdown
            cur.execute("SELECT customer_id, CONCAT(first_name, ' ', last_name) as full_name FROM customer ORDER BY first_name")
            customers = cur.fetchall()

            # 3. Staff removed entirely
            return payment, customers

    def update_payment(self, payment_id, data):
        """
        Updates the payment record.
        REMOVED: staff_id update.
        """
        sql = """
            UPDATE payment 
            SET customer_id = %s,
                amount = %s,
                payment_date = %s,
                payment_method = %s
            WHERE payment_id = %s
        """
        
        clean_date = data['payment_date'].replace('T', ' ')

        params = (
            data['customer_id'],
            # staff_id removed here
            data['amount'],
            clean_date,
            data['payment_method'],
            payment_id
        )

        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, params)
            cn.commit()

class Rentals:
    """Data-access helpers for the rental table."""
    def __init__(self, connection_factory: Callable[[], mysql.connector.MySQLConnection]):
        self.connection_factory = connection_factory

    def search(self, q=None, status=None, page=1, page_size=20):
        """
        Searching in Rental Tables.
        status: 'returned', 'not_returned' or None
        q: Customer name or film name search
        """
        offset = (page - 1) * page_size
        where = []
        params = []


        if status == 'not_returned':
            where.append("r.return_date IS NULL")
        elif status == 'returned':
            where.append("r.return_date IS NOT NULL")

        if q:
            like_q = f"%{q}%"
            where.append("(CONCAT(c.first_name, ' ', c.last_name) LIKE %s OR f.title LIKE %s)")
            params.extend([like_q, like_q])

        where_clause = ("WHERE " + " AND ".join(where)) if where else ""

        sql = f"""
            SELECT 
                r.rental_id, r.rental_date, r.return_date,
                c.customer_id, c.first_name, c.last_name,
                f.film_id, f.title
            FROM rental r
            JOIN customer c ON r.customer_id = c.customer_id
            JOIN film f ON r.film_id = f.film_id
            {where_clause}
            ORDER BY r.rental_date DESC
            LIMIT %s OFFSET %s
        """
        params.extend([page_size, offset])

        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, params)
            return _dict_rows(cur)

    def get(self, rental_id: int):
        """Only one rental but with details"""
        sql = """
            SELECT 
                r.*,
                c.first_name, c.last_name,
                f.title
            FROM rental r
            JOIN customer c ON r.customer_id = c.customer_id
            JOIN film f ON r.film_id = f.film_id
            WHERE r.rental_id = %s
        """
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, (rental_id,))
            rows = _dict_rows(cur)
            return rows[0] if rows else None

    def add(self, customer_id, film_id):
        """New rental (Today's date)"""
        sql = """
            INSERT INTO rental (rental_date, film_id, customer_id)
            VALUES (NOW(), %s, %s)
        """
        with self.connection_factory() as cn, cn.cursor() as cur:
            cur.execute(sql, (film_id, customer_id))

    def return_film(self, rental_id):
        """Return film"""
        sql = """
            UPDATE rental
            SET return_date = NOW()
            WHERE rental_id = %s
        """
        with self.connection_factory() as cn, cn.cursor() as cur:

            cur.execute(sql, (rental_id,))



