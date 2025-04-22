from typing import Optional


def query(
    *,
    score_modes: Optional[list[int]] = None,
    map_modes: Optional[list[int]] = None,
    score_statuses: Optional[list[int]] = None,
    map_statuses: Optional[list[int]] = None,
    user_ids: Optional[list[int]] = None,
    time_after: Optional[int] = None,
    time_before: Optional[int] = None,
    update_failed_scores: Optional[bool] = False,
):
    _q = (
        """
    WITH MAX AS (
        SELECT
            userid,
            mode,
            max(pp) AS max_pp,
            map_md5
        FROM
            scores
        WHERE
            grade != 'F' -- make sure early exited scores are not in considering
            -- AND max_pp > 0 -- no perf gain
        GROUP BY
            userid,
            mode,
            map_md5
    ),
    RAW_MAX_PPS AS (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY s2.userid,
                s2.mode,
                s2.map_md5
                ORDER BY
                    s2.pp DESC,
                    s2.score DESC,
                    s2.id DESC
            ) AS rn
        FROM
            scores s2
            INNER JOIN MAX ON
            s2.userid = MAX.userid
            AND s2.mode = MAX.mode
            AND s2.pp = MAX.max_pp
            AND s2.map_md5 = MAX.map_md5
        WHERE
            s2.pp > 0
            AND s2.grade != 'F' -- edge case: same pp, failed scores, higher score
    ),
    MAX_PPS AS (
        SELECT
            id
        FROM
            RAW_MAX_PPS
        WHERE
            rn = 1
    )
    UPDATE
        scores s
    """
        + ("LEFT JOIN maps m ON s.map_md5 = m.md5" if map_statuses or map_modes else "")
        + """
    SET
        s.status = CASE
    """
        + ("WHEN s.grade = 'F' THEN 0" if update_failed_scores else "")
        + """
            WHEN s.id IN (
                select
                    id
                from
                    MAX_PPS
            ) THEN 2
            WHEN s.status = 2 THEN 1
            ELSE s.status
        END
    WHERE
    """
        + "\n".join(
            v
            for v in [
                "s.grade != 'F'" if not update_failed_scores else "1 = 1",
                "AND s.status IN :score_statuses" if score_statuses else "s.status > 0",
                "AND s.mode IN :score_modes" if score_modes else "",
                "AND s.userid IN :user_ids" if user_ids else "",
                "AND m.status IN :map_statuses" if map_statuses else "",
                "AND m.mode IN :map_modes" if map_modes else "",
                (
                    "AND s.time BETWEEN :time_after AND :time_before"
                    if time_after is not None and time_before is not None
                    else (
                        "AND s.time >= :time_after"
                        if time_after is not None
                        else (
                            "AND s.time <= :time_before"
                            if time_before is not None
                            else ""
                        )
                    )
                ),
            ]
            if v is not None and v != ""
        )
    )
    return _q, {
        "score_statuses": score_statuses,
        "score_modes": score_modes,
        "user_ids": user_ids,
        "map_statuses": map_statuses,
        "map_modes": map_modes,
        "time_after": time_after,
        "time_before": time_before,
    }


if __name__ == "__main__":
    q, p = query(
        score_modes=[0, 1],
        # map_modes=[0, 1],
        score_statuses=[0, 1],
        # map_statuses=[0, 1],
        user_ids=[123456789],
        time_after=0,
        time_before=1000000000,
        update_failed_scores=False,
    )

    print(q, p)
