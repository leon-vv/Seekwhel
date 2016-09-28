SELECT "name"
FROM Person
WHERE CHAR_LENGTH(COALESCE(parent, "")) > ANY
(
	SELECT id
	FROM Post
)
