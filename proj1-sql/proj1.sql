-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era) AS
    SELECT MAX(era)
    FROM pitching

;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE namefirst LIKE '% %'
  ORDER BY namefirst, namelast
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM people
  GROUP BY birthyear
  ORDER BY birthyear
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height), COUNT(*)
  FROM people
  GROUP BY birthyear
  HAVING AVG(height) > 70
  ORDER BY birthyear
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, playerid, yearid
  FROM people NATURAL JOIN HallofFame
  WHERE HallofFame.inducted = 'Y'
  ORDER BY yearid DESC, playerid ASC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT namefirst, namelast, h.playerid, c.schoolid, h.yearid
  FROM people p, HallofFame h, schools s, collegeplaying c
  WHERE p.playerid = h.playerid
  AND p.playerid = c.playerid
  AND c.schoolid = s.schoolid
  AND s.schoolState = 'CA'
  AND h.inducted = 'Y'
  ORDER BY h.yearid DESC, c.schoolid, h.playerid
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT h.playerid, namefirst, namelast, c.schoolid
  FROM HallofFame h, people p LEFT OUTER JOIN Collegeplaying c
  ON p.playerid = c.playerid
  WHERE p.playerid = h.playerid
  AND h.inducted = 'Y'
  ORDER BY h.playerid DESC, c.schoolid
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT p.playerid, p.namefirst, p.namelast, b.yearid,
        (b.H - b.H2B - b.H3B - b.HR + 2*b.h2b + 3*b.h3b + 4*b.hr) / (cast(b.AB as FlOAT)) slg
  FROM people p INNER JOIN batting b
  ON p.playerid = b.playerid
  WHERE b.AB > 50
  ORDER BY slg DESC, b.yearid, p.playerid
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT p.playerid, p.namefirst, p.namelast,
    SUM(b.H - b.H2B - b.H3B - b.HR + 2*b.h2b + 3*b.h3b + 4*b.hr) / cast(SUM(b.AB) as FlOAT) lslg
  FROM people p INNER JOIN batting b on b.playerid = p.playerid
  WHERE b.AB > 0
  GROUP BY p.playerid
  HAVING(SUM(b.AB) > 50)
  ORDER BY lslg DESC, p.playerid
  LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  WITH s AS (
    SELECT p.playerid,
        SUM(b.H - b.H2B - b.H3B - b.HR + 2*b.h2b + 3*b.h3b + 4*b.hr) / cast(SUM(b.AB) as FlOAT) lslg
    FROM people p INNER JOIN batting b
    ON b.playerid = p.playerid
    WHERE b.AB > 0
    GROUP BY p.playerid
    HAVING(SUM(b.AB) > 50)
  )
  SELECT p.namefirst, p.namelast, s.lslg
  FROM people p INNER JOIN s
  ON p.playerid = s.playerid
  WHERE s.lslg > (SELECT lslg FROM s WHERE playerid = 'mayswi01')

;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT yearid, MIN(salary), MAX(salary), AVG(salary) -- replace this line
  FROM salaries
  GROUP BY yearid
  ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  WITH r AS (
    SELECT MIN(salary) low, MAX(salary) high,
    CAST (((MAX(salary) - MIN(salary))/10) AS INT) bkt
    FROM salaries
    WHERE yearid = 2016
  )
  SELECT binid, low + bkt * binid, low + bkt * (binid + 1), count(*)
  FROM binids b, salaries s, r
  WHERE (salary BETWEEN low + bkt * binid AND low + bkt * (binid + 1))
  AND yearid = 2016
  GROUP BY binid
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH prev AS (
    SELECT MIN(salary) low, MAX(salary) high, AVG(salary) avg, yearid + 1 yid
    FROM salaries
    GROUP BY yid
  ),
  curr AS (
  SELECT MIN(salary) low, MAX(salary) high, AVG(salary) avg, yearid
  FROM salaries
  GROUP BY yearid

  )
  SELECT c.yearid, c.low - p.low mindiff, c.high - p.high maxdiff, c.avg - p.avg avgdiff
  FROM prev p, curr c
  WHERE p.yid = c.yearid
  ORDER BY c.yearid
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid) AS
    SELECT p.playerid, namefirst, namelast, MAX(salary), yearid
    FROM people p, salaries s
    WHERE p.playerid = s.playerid
    AND yearid IN (2000, 2001)
    GROUP BY yearid

;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT a.teamid team, (MAX(salary)-MIN(salary)) diffAvg
  FROM salaries s, allstarfull a
  WHERE s.playerid = a.playerid
  AND a.yearid = 2016
  AND s.yearid = a.yearid
  GROUP BY a.teamid
;

