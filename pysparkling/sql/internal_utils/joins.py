
INNER_JOIN = "inner"
CROSS_JOIN = "cross"
FULL_JOIN = "full"
LEFT_JOIN = "left"
RIGHT_JOIN = "right"
LEFT_SEMI_JOIN = "leftsemi"
LEFT_ANTI_JOIN = "leftanti"

JOIN_TYPES = dict(
    inner=INNER_JOIN,
    cross=CROSS_JOIN,
    outer=FULL_JOIN,
    full=FULL_JOIN,
    fullouter=FULL_JOIN,
    left=LEFT_JOIN,
    leftouter=LEFT_JOIN,
    right=RIGHT_JOIN,
    rightouter=RIGHT_JOIN,
    leftsemi=LEFT_SEMI_JOIN,
    leftanti=LEFT_ANTI_JOIN,
)
