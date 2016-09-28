SELECT "name"
FROM person
WHERE CHAR_LENGTH(COALESCE(parent, '')) > ANY
(
	SELECT "id"
	FROM post
)
