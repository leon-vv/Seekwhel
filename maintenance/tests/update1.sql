UPDATE person
SET
	"name" =
		'child of ' || COALESCE(parent, ''),
	parent =
		DEFAULT
WHERE
	CHAR_LENGTH("name") > 15
