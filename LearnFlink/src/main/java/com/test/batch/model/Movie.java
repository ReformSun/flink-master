package com.test.batch.model;

public class Movie {
	/**
	 * 此数据集中电影的唯一电影ID。
	 */
	private Long movieId;
	/**
	 * 电影的标题
	 */
	private String title;
	/**
	 * 将每部电影其他电影区分开的类型列表
	 */
	private String genres;

	public Movie(Long movieId, String title, String genres) {
		this.movieId = movieId;
		this.title = title;
		this.genres = genres;
	}

	public Long getMovieId() {
		return movieId;
	}

	public String getTitle() {
		return title;
	}

	public String getGenres() {
		return genres;
	}

	@Override
	public String toString() {
		return movieId + "," + title + "," + genres;
	}
}
