from sqlalchemy import (
    Table, MetaData, Column, Integer, String, Date, DateTime,
    ForeignKey
)
from sqlalchemy.orm import mapper, relationship

from CS235flix.domain.model import User, Movie, Review, Genre, Actor, Director

metadata = MetaData()

users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('username', String(255), unique=True, nullable=False),
    Column('password', String(255), nullable=False)
)

reviews = Table(
    'reviews', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('user_id', ForeignKey('users.id')),
    Column('movie_rank', ForeignKey('movies.rank')),
    Column('review_text', String(1024), nullable=False),
    Column('timestamp', DateTime, nullable=False)
)

movies = Table(
    'movies', metadata,
    Column('rank', Integer, primary_key=True),
    Column('year', Integer, nullable=False),
    Column('title', String(255), nullable=False),
    Column('description', String(1024), nullable=False),
    Column('poster', String(255), nullable=True)
)

genres = Table(
    'genres', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(64), nullable=False)
)

movie_genres = Table(
    'movie_genres', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('movie_rank', ForeignKey('movies.rank')),
    Column('genre_id', ForeignKey('genres.id'))
)

actors = Table(
    "actors", metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(64), nullable=False)
)

directors = Table(
    "director", metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(64), nullable=False)
)




def map_model_to_tables():
    mapper(User, users, properties={
        '_User__username': users.c.username,
        '_User__password': users.c.password,
        '_User__reviews': relationship(Review, backref='_user')
    })
    mapper(Review, reviews, properties={
        '_Review__review': reviews.c.review_text,
        '_Review__timestamp': reviews.c.timestamp
    })

    movies_mapper = mapper(Movie, movies, properties={
        '_Movie__rank': movies.c.rank,
        '_Movie__release_year': movies.c.year,
        '_Movie__title': movies.c.title,
        '_Movie__poster': movies.c.poster,
        '_Movie__description': movies.c.description,
        '_Movie__reviews': relationship(Review, backref='_movie')
    })

    mapper(Genre, genres, properties={
        '_Genre__genre_name': genres.c.name,
        '_Genre__movies': relationship(
            movies_mapper,
            secondary=movie_genres,
            backref="_tags"
        )
    })
    mapper(Actor, actors, properties={
        '_Actor__actor_full_name': actors.c.name,
    })
    mapper(Director, directors, properties={
        '_Director__director_full_name': directors.c.name,
    })
