WITH max_pp_per_map AS (
    SELECT
        s.userid,
        s.map_md5,
        s.mode,
        MAX(s.pp) AS maxPP
    FROM
        scores s
        INNER JOIN maps m ON m.md5 = s.map_md5
    WHERE
        m.status IN (2, 3)
        AND s.grade != "F"
    GROUP BY
        s.userid,
        s.map_md5,
        s.mode
),
ordered_pp AS (
    SELECT
        mp.userid,
        mp.mode,
        mp.maxPP AS pp,
        s2.acc,
        ROW_NUMBER() OVER (
            PARTITION BY mp.userid,
            mp.map_md5,
            mp.mode
            ORDER BY
                s2.acc DESC,
                s2.id DESC
        ) AS rn_per_map
    FROM
        max_pp_per_map mp
        INNER JOIN scores s2 ON s2.userid = mp.userid
        AND s2.map_md5 = mp.map_md5
        AND s2.mode = mp.mode
        AND s2.pp = mp.maxPP
    WHERE
        mp.maxPP > 0
),
filtered_scores AS (
    SELECT
        *
    FROM
        ordered_pp
    WHERE
        rn_per_map = 1
),
global_ranking AS (
    SELECT
        userid,
        mode,
        pp,
        acc,
        ROW_NUMBER() OVER (
            PARTITION BY userid,
            mode
            ORDER BY
                pp DESC
        ) AS global_rank
    FROM
        filtered_scores
),
user_calc AS (
    SELECT
        userid,
        mode,
        COUNT(*) AS count,
        SUM(POW(0.95, global_rank - 1) * pp) AS weightedPP,
        (1 - POW(0.9994, COUNT(*))) * 416.6667 AS bnsPP,
        SUM(POW(0.95, global_rank - 1) * acc) / SUM(POW(0.95, global_rank - 1)) AS acc
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
)
UPDATE
    stats s
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
    );
WHERE
    s.mode IN :modes