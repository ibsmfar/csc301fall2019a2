# csc301fall2019a2

This is a patch you can use to correct errors in the validation script

twitch.py - file is identical except for adding the sort_keys param to occurrences of json.dumps

validate.sh - adds a couple of uses of sed to get rid of unicode prefix

validation/* - rebuilt test files to reflect ordering change