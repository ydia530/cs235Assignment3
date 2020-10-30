import csv

from CS235flix.domain.model import Movie, Genre, Actor, Director


class MovieFileCSVReader:
    def __init__(self, file_name: str):
        self.__file_name = file_name
        self.dataset_of_movies = []
        self.dataset_of_actors = []
        self.dataset_of_directors = []
        self.dataset_of_genres = []

    def read_csv_file(self):
        with open(self.__file_name, mode='r', encoding='utf-8-sig') as csvfile:
            movie_file_reader = csv.DictReader(csvfile)
            index = 0
            for row in movie_file_reader:
                movie = Movie(row["Title"], int(row["Year"]))
                if movie not in self.dataset_of_movies:
                    movie.description = row["Description"]
                    movie.poster = row["Poster"]
                    self.dataset_of_movies.append(movie)
                    movie.genres = row['Genres'].split(",")

                for a in row["Actors"].split(","):
                    actor = Actor(a)
                    if actor not in self.dataset_of_actors:
                        self.dataset_of_actors.append(actor)
                director = Director(row["Director"])
                if director not in self.dataset_of_directors:
                    self.dataset_of_directors.append(director)

                for a in row["Genre"].split(","):
                    genre = Genre(a)
                    if genre not in self.dataset_of_genres:
                        self.dataset_of_genres.append(genre)
                index += 1
