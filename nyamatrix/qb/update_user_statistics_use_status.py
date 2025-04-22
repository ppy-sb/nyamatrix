from typing import Optional, List

CALC_PP_CTES = """
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
    bests AS (
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
            bests
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
    )"""

CONCRETE_STATS_CTES = """
    concrete_stats AS (
        SELECT
            s.userid,
            s.mode,
            SUM(s.score) AS total_score,
            SUM(s.time_elapsed) AS play_time,
            COUNT(s.grade = "XH") AS xh_count,
            COUNT(s.grade = "X") AS x_count,
            COUNT(s.grade = "S") AS s_count,
            COUNT(s.grade = "A") AS a_count
        FROM
            scores s
        GROUP BY
            s.userid,
            s.mode
    )"""

RANKED_STATS_CTES = """
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
    )"""


def query(
    *,
    calc_pp: Optional[bool] = None,
    slow_statistics: Optional[bool] = None,
    very_slow_statistics: Optional[bool] = None,
    modes: Optional[List[int]] = None,
):
    """
    Update user statistics based on the scores table.
    ~1s for 1 user, ~2s for full recalc
    Slow statistics will calculate the total score and play time for each user. ~1s for 1 user, ~3s for full recalc
    Very slow statistics will calculate the ranked score for each user. ~7s for 1 user, ~10s for full recalc
    """

    if not calc_pp and not slow_statistics and not very_slow_statistics:
        return None, {}

    ctes = (
        v
        for v in [
            "dummy AS (SELECT 1)",
            CALC_PP_CTES if calc_pp else None,
            CONCRETE_STATS_CTES if slow_statistics else None,
            RANKED_STATS_CTES if very_slow_statistics else None,
        ]
        if v is not None
    )

    join_tables = (
        v
        for v in [
            (
                "LEFT JOIN calculated c ON s.id = c.userId AND s.mode = c.mode"
                if calc_pp
                else None
            ),
            (
                "LEFT JOIN concrete_stats cs ON s.id = cs.userId AND s.mode = cs.mode"
                if slow_statistics
                else None
            ),
            (
                "LEFT JOIN ranked_stats rs ON s.id = rs.userId AND s.mode = rs.mode"
                if very_slow_statistics
                else None
            ),
        ]
        if v is not None
    )

    updates = (
        v
        for v in [
            (
                ("s.pp = COALESCE(c.pp,0)," "s.acc = COALESCE(c.acc,0)")
                if calc_pp
                else None
            ),
            (
                (
                    "s.tscore = COALESCE(cs.total_score,0),"
                    "s.playtime = COALESCE(cs.play_time,0),"
                    "s.xh_count = COALESCE(cs.xh_count,0),"
                    "s.x_count = COALESCE(cs.x_count,0),"
                    "s.s_count = COALESCE(cs.s_count,0),"
                    "s.a_count = COALESCE(cs.a_count,0)"
                )
                if slow_statistics
                else None
            ),
            (
                "s.rscore = COALESCE(rs.ranked_score,0)"
                if very_slow_statistics
                else None
            ),
        ]
        if v is not None
    )

    _q = (
        f"""
    WITH {", ".join(ctes)}
    UPDATE stats s
    {" ".join(join_tables)}
    SET
        {", ".join(updates)}
"""
        + "WHERE s.mode IN :modes"
        if modes
        else ""
    )

    return _q, {"modes": modes}


if __name__ == "__main__":
    print(
        *query(
            calc_pp=True,
            slow_statistics=True,
            very_slow_statistics=True,
            modes=[0, 1, 2],
        )
    )

# Full query:
"""--sql
WITH
    dummy AS (
        SELECT
            1
    ),
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
    bests AS (
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
            bests
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
            SUM(s.time_elapsed) AS play_time,
            COUNT(s.grade = "XH") AS xh_count,
            COUNT(s.grade = "X") AS x_count,
            COUNT(s.grade = "S") AS s_count,
            COUNT(s.grade = "A") AS a_count
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
INNER JOIN calculated c ON s.id = c.userId
AND s.mode = c.mode
INNER JOIN concrete_stats cs ON s.id = cs.userId
AND s.mode = cs.mode
INNER JOIN ranked_stats rs ON s.id = rs.userId
AND s.mode = rs.mode
SET
    s.pp = COALESCE(c.pp, 0),
    s.acc = COALESCE(c.acc, 0),
    s.tscore = COALESCE(cs.total_score, 0),
    s.playtime = COALESCE(cs.play_time, 0),
    s.xh_count = COALESCE(cs.xh_count, 0),
    s.x_count = COALESCE(cs.x_count, 0),
    s.s_count = COALESCE(cs.s_count, 0),
    s.a_count = COALESCE(cs.a_count, 0),
    s.rscore = COALESCE(rs.ranked_score, 0)
"""
