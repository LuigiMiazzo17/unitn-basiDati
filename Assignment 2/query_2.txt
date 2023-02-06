WITH
    winners AS
        (
            SELECT DISTINCT movies.title, movies.year, COUNT(movieawards.award)
            FROM movies INNER JOIN movieawards ON movies.year = movieawards.year AND movies.title = movieawards.title
            WHERE movieawards.result = 'won'
            GROUP BY movies.year, movies.title
            HAVING COUNT(movieawards.award) >= 3
        )
SELECT DISTINCT winners.title, winners.year FROM winners
WHERE winners.year = (SELECT MAX(winners.year) FROM winners)
ORDER BY winners.title, winners.year;