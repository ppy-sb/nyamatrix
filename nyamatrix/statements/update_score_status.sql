WITH MAX AS (
    SELECT
        userid,
        mode,
        max(pp) AS max_pp
    FROM
        scores
    WHERE
        score > 0
    GROUP BY
        userid,
        mode,
        map_md5
),
MAX_PPS AS (
    SELECT
        id
    FROM
        scores s2
        INNER JOIN MAX ON s2.userid = MAX.userid
        AND s2.mode = MAX.mode
        AND s2.pp = MAX.max_pp
    WHERE
        s2.pp > 0
)
UPDATE
    scores
SET
    status = CASE
        WHEN id IN (
            select
                id
            from
                MAX_PPS
        ) THEN 2
        WHEN status = 2 THEN 1
        ELSE status
    END