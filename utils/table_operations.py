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
