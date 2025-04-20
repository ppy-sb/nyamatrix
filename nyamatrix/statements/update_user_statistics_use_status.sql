WITH
    ordered_pp AS (
        SELECT
            s.userid,
            s.mode,
            s.pp,
            s.acc
        FROM
            scores s
        WHERE
            s.status = 2
            AND s.pp > 0
        ORDER BY
            s.pp DESC,
            s.acc DESC,
            s.id DESC
    ),
    global_ranking AS (
        SELECT
            userid,
            mode,
            pp,
            acc,
            ROW_NUMBER() OVER (
                PARTITION BY
                    userid,
                    mode
                ORDER BY
                    pp DESC
            ) AS global_rank
        FROM
            ordered_pp
    ),
    user_calc AS (
        SELECT
            userid,
            mode,
            COUNT(*) AS count,
            SUM(POW (0.95, global_rank - 1) * pp) AS weightedPP,
            (1 - POW (0.9994, COUNT(*))) * 416.6667 AS bnsPP,
            SUM(POW (0.95, global_rank - 1) * acc) / SUM(POW (0.95, global_rank - 1)) AS acc
        FROM
            global_ranking
        GROUP BY
            userid,
            mode
    ),
    calculated AS (
        SELECT
            *,
            weightedPP + bnsPP AS pp
        FROM
            user_calc
    ),
    concrete_stats AS (
        SELECT
            s.userid,
            s.mode,
            SUM(s.score) AS total_score,
            SUM(s.time_elapsed) AS play_time
        FROM
            scores s
        GROUP BY
            s.userid,
            s.mode
    ),
    ranked_stats AS (
        SELECT
            s.userid,
            s.mode,
            SUM(s.score) AS ranked_score
        FROM
            scores s
            LEFT JOIN maps m ON s.map_md5 = m.md5
        WHERE
            m.status IN (2, 3)
            AND s.status = 2
        GROUP BY
            s.userid,
            s.mode
    )
UPDATE stats s
SET
    s.pp = COALESCE(
        (
            SELECT
                pp
            FROM
                calculated c
            WHERE
                c.userId = s.id
                AND c.mode = s.mode
        ),
        0
    ),
    s.acc = COALESCE(
        (
            SELECT
                acc
            FROM
                calculated c
            WHERE
                c.userId = s.id
                AND c.mode = s.mode
        ),
        0
    )
WHERE
    s.mode IN :modes