import csv
import os

from typing import List
from CS235flix.adapters.data.MovieFileReader import MovieFileCSVReader
from sqlalchemy import desc, asc
from sqlalchemy.engine import Engine
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from werkzeug.security import generate_password_hash

from sqlalchemy.orm import scoped_session
from flask import _app_ctx_stack

from CS235flix.domain.model import User, Movie, Review, Genre
from CS235flix.adapters.repository import AbstractRepository

genres = None


class SessionContextManager:
    def __init__(self, session_factory):
        self.__session_factory = session_factory
        self.__session = scoped_session(self.__session_factory, scopefunc=_app_ctx_stack.__ident_func__)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.rollback()

    @property
    def session(self):
        return self.__session

    def commit(self):
        self.__session.commit()

    def rollback(self):
        self.__session.rollback()

    def reset_session(self):
        # this method can be used e.g. to allow Flask to start a new session for each http request,
        # via the 'before_request' callback
        self.close_current_session()
        self.__session = scoped_session(self.__session_factory, scopefunc=_app_ctx_stack.__rankent_func__)

    def close_current_session(self):
        if not self.__session is None:
            self.__session.close()


class SqlAlchemyRepository(AbstractRepository):

    def __init__(self, session_factory):
        self._session_cm = SessionContextManager(session_factory)

    def close_session(self):
        self._session_cm.close_current_session()

    def reset_session(self):
        self._session_cm.reset_session()

    def add_user(self, user: User):
        with self._session_cm as scm:
            scm.session.add(user)
            scm.commit()

    def get_user(self, username) -> User:
        user = None
        try:
            user = self._session_cm.session.query(User).filter_by(_username=username).one()
        except NoResultFound:
            # Ignore any exception and return None.
            pass

        return user

    def get_reviews(self):
        review = None
        try:
            review = self._session_cm.session.query(Review).all()
        except NoResultFound:
            # Ignore any exception and return None.
            pass

        return review

    def add_movie(self, movie: Movie):
        with self._session_cm as scm:
            scm.session.add(movie)
            scm.commit()

    def add_genre(self, genre: Genre):
        with self._session_cm as scm:
            scm.session.add(genre)
            scm.commit()

    def get_movie(self, rank: int) -> Movie:
        movie = None
        try:
            movie = self._session_cm.session.query(Movie).filter(Movie._rank == rank).one()
        except NoResultFound:
            # Ignore any exception and return None.
            pass

        return movie

    def get_number_of_movies(self):
        number_of_movies = self._session_cm.session.query(Movie).count()
        return number_of_movies

    def get_first_movie(self):
        movie = self._session_cm.session.query(Movie).first()
        return movie

    def get_last_movie(self):
        movie = self._session_cm.session.query(Movie).order_by(desc(Movie._rank)).first()
        return movie

    def get_year_of_previous_movie(self, movie: Movie):
        result = None
        prev = self._session_cm.session.query(movie).filter(Movie._year < movie.release_year).order_by(
            desc(Movie._year)).first()

        if prev is not None:
            result = prev.year

        return result

    def get_year_of_next_movie(self, movie: Movie):
        result = None
        next = self._session_cm.session.query(movie).filter(Movie._year > movie.release_year).order_by(
            asc(Movie._year)).first()

        if next is not None:
            result = next.year

        return result

    def get_movies_by_year(self, target_year) -> List[Movie]:
        if target_year is None:
            movies = self._session_cm.session.query(Movie).all()
            return movies
        else:
            # Return movies matching target_year; return an empty list if there are no matches.
            movies = self._session_cm.session.query(Movie).filter(Movie.release_year == target_year).all()
            return movies

    def get_genres(self) -> List[Genre]:
        genres = self._session_cm.session.query(Genre).all()
        return genres

    def get_movies_by_rank(self, rank_list):

        movies = self._session_cm.session.query(Movie).filter(Movie._Movie__rank.in_(rank_list)).all()
        return movies

    def get_movie_ranks_for_genre(self, genre_name: str):
        movie_ranks = []

        # Use native SQL to retrieve movie ranks, since there is no mapped class for the movie_genres table.
        row = self._session_cm.session.execute('SELECT id FROM genres WHERE name = :genre_name',
                                               {'genre_name': genre_name}).fetchone()

        if row is None:
            # No genre with the name genre_name - create an empty list.
            movie_ranks = list()
        else:
            genre_rank = row[0]

            # Retrieve movie ranks of movies associated with the genre.
            movie_ranks = self._session_cm.session.execute(
                'SELECT movie_rank FROM movie_genres WHERE genre_rank = :genre_rank ORDER BY movie_rank ASC',
                {'genre_rank': genre_rank}
            ).fetchall()
            movie_ranks = [rank[0] for rank in movie_ranks]

        return movie_ranks

    def add_review(self, review: Review):
        super().add_review(review)
        with self._session_cm as scm:
            scm.session.add(review)
            scm.commit()


def get_genre_records():
    genre_records = list()
    genre_key = 0

    for genre in genres.keys():
        genre_key = genre_key + 1
        genre_records.append((genre_key, genre))
    return genre_records


def movie_genres_generator():
    movie_genres_key = 0
    genre_key = 0

    for genre in genres.keys():
        genre_key = genre_key + 1
        for movie_key in genres[genre]:
            movie_genres_key = movie_genres_key + 1
            yield movie_genres_key, movie_key, genre_key


def generic_generator(filename, post_process=None):
    with open(filename) as infile:
        reader = csv.reader(infile)

        # Read first line of the CSV file.
        next(reader)

        # Read remaining rows from the CSV file.
        for row in reader:
            # Strip any leading/trailing white space from data read.
            row = [item.strip() for item in row]

            if post_process is not None:
                row = post_process(row)
            yield row


def process_user(user_row):
    user_row[2] = generate_password_hash(user_row[2])
    return user_row


def populate(session_factory, data_path: str):
    filename = os.path.join(data_path, "Data1000MoviesWithImage")
    actor_and_director_file_reader = MovieFileCSVReader(filename)
    actor_and_director_file_reader.read_csv_file()

    session = session_factory()

    global genres
    genres = dict()

    for genre in actor_and_director_file_reader.dataset_of_genres:
        genres[genre] = []
        session.add(genre)

    for actor in actor_and_director_file_reader.dataset_of_actors:
        session.add(actor)

    for director in actor_and_director_file_reader.dataset_of_directors:
        session.add(director)

    for movie in actor_and_director_file_reader.dataset_of_movies:
        session.add(movie)

    session.commit()
