Classify (cleaned) tokens according to their first letter:
-- tokens starting with 'a' to 'i' are EARLY
-- tokens starting with 'j' to 'r' are MIDDLE
-- tokens starting with 's' to 'z' are LATE

We are interested in whether the length of a token depends on 
whether it is EARLY, MIDDLE, or LATE, e.g. whether EARLY tokens 
tend to be shorter or longer than LATE tokens.

First step:  generate ('EARLY', <avg-length>) etc.