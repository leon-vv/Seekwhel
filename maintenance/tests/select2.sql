SELECT "name"
	, parent
FROM person
FULL OUTER JOIN post ON "name" = person_name
WHERE
	(CHAR_LENGTH(COALESCE(parent, '') || "name") > 15)
	AND ((CHAR_LENGTH(COALESCE(parent, '') || "name") + CHAR_LENGTH("content")) = 33)
