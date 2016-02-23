# serengeti-analysis-scripts
Various PyMongo and other scripts for analysing Serengeti DB

My general approach is that each script will write its output to a pickle file, for use in the next script. This means you can re-run part of the analysis as you tweak the scripts without having to run everything again from scratch.

Order to run the scripts:

1. Run some tests to see what classifications you have:
    1. (optional) run `python validate-classifications-by-date-full-iteration.py`. This checks the dates of all classifications
    2. (optional) run `python validate-classifications-by-date-paged.py`. This checks the dates of all classifications using a different approach

2. Create season mapping:
    1. First run `python get-seasons.py`. This generates a mapping of which subject is in each season (`subject_season_map.p`).
    2. (optional) You can now run `python validate-subject-season-map.py` which will load the season map pickle file then summarise it
    3. (optional) You can now run `python validate-seasons-present-in-classifications.py`. This simply checks which seasons' images have got classifications present, and outputs the results to the screen.

3. Inspect the mix of users in each season:
    1. You can now run `python get-user-mix.py`. This will scan through the classifications DB, referencing the subject/season mapping, and build up arrays of which users were encountered in each season and how many anonymous users were in each season. This will be written to two new pickle files (`known_users.p` and `anon_users_counts.p`).
    2. (optional) You can now run `python validate-known-users.py` which will load the known users file and summarise it.

4. Generate a CSV summary:
    1. You can now run `python get-summary.py`. This will load the known users and anon users file and generate a CSV containing the new, returning and anonymous users per season.



