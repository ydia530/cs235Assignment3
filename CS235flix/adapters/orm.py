from sqlalchemy import (
    Table, MetaData, Column, Integer, String, Date, DateTime,
    ForeignKey
)
from sqlalchemy.orm import mapper, relationship

from CS235flix.domain import model

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

articles = Table(
    'movies', metadata,
    Column('rank', Integer, primary_key=True),
    Column('year', Integer, nullable=False),
    Column('title', String(255), nullable=False),
    Column('description', String(1024), nullable=False),
    Column('poster', String(255), nullable=False)
)

genres = Table(
    'genres', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(64), nullable=False)
)

movie_genres = Table(
    'movie_gneres', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('movie_rank', ForeignKey('movies.rank')),
    Column('genre_id', ForeignKey('gneres.id'))
)


def map_model_to_tables():
    mapper(model.User, users, properties={
        '_username': users.c.username,
        '_password': users.c.password,
        '_comments': relationship(model.Comment, backref='_user')
    })
    mapper(model.Comment, comments, properties={
        '_comment': comments.c.comment,
        '_timestamp': comments.c.timestamp
    })
    articles_mapper = mapper(model.Article, articles, properties={
        '_id': articles.c.id,
        '_date': articles.c.date,
        '_title': articles.c.title,
        '_first_para': articles.c.first_para,
        '_hyperlink': articles.c.hyperlink,
        '_image_hyperlink': articles.c.image_hyperlink,
        '_comments': relationship(model.Comment, backref='_article')
    })
    mapper(model.Tag, tags, properties={
        '_tag_name': tags.c.name,
        '_tagged_articles': relationship(
            articles_mapper,
            secondary=article_tags,
            backref="_tags"
        )
    })
