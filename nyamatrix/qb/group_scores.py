from typing import Optional


def count(
    *,
    score_modes: Optional[list[int]] = None,
    map_modes: Optional[list[int]] = None,
    score_statuses: Optional[list[int]] = None,
    map_statuses: Optional[list[int]] = None,
    user_ids: Optional[list[int]] = None,
    time_after: Optional[int] = None,
    time_before: Optional[int] = None,
):
    _q = """
    SELECT
        count(*)
    FROM
        scores s
        INNER JOIN maps m ON s.map_md5 = m.md5
    WHERE
    """ + "\n".join(
        v
        for v in [
            "s.status IN :score_statuses" if score_statuses else "s.status > 0",
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
                    else ("AND s.time <= :time_before" if time_before is not None else "")
                )
            ),
        ]
        if v is not None and v != ""
    )
    return _q, {
        "score_statuses": score_statuses,
        "map_statuses": map_statuses,
        "map_modes": map_modes,
        "score_modes": score_modes,
        "user_ids": user_ids,
        "time_after": time_after,
        "time_before": time_before,
    }


def query(
    *,
    score_modes: Optional[list[int]] = None,
    map_modes: Optional[list[int]] = None,
    score_statuses: Optional[list[int]] = None,
    map_statuses: Optional[list[int]] = None,
    user_ids: Optional[list[int]] = None,
    time_after: Optional[int] = None,
    time_before: Optional[int] = None,
):
    _q = (
        """
    SELECT
        m.id,
        s.mode,
        JSON_ARRAYAGG(
            JSON_ARRAY(
                s.id,
                s.mods,
                s.max_combo,
                s.ngeki,
                s.n300,
                s.nkatu,
                s.n100,
                s.n50,
                s.nmiss
            )
        ) ss
    FROM
        scores s
        INNER JOIN maps m ON s.map_md5 = m.md5
    WHERE
    """
        + "\n".join(
            v
            for v in [
                "s.status IN :score_statuses" if score_statuses else "s.status > 0",
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
                        else ("AND s.time <= :time_before" if time_before is not None else "")
                    )
                ),
            ]
            if v is not None and v != ""
        )
        + """
    GROUP BY
      s.map_md5,
      s.mode"""
    )
    return _q, {
        "score_statuses": score_statuses,
        "map_statuses": map_statuses,
        "map_modes": map_modes,
        "score_modes": score_modes,
        "user_ids": user_ids,
        "time_after": time_after,
        "time_before": time_before,
    }


if __name__ == "__main__":
    q, p = query(
        score_modes=[0, 1],
        map_modes=[0, 1],
        score_statuses=[0, 1],
        map_statuses=[0, 1],
        user_ids=[123456789],
        time_after=0,
        time_before=1000000000,
    )

    print(q, p)
