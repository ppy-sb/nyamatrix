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
    s.status > 0
    AND s.mode IN :modes
GROUP BY
    s.map_md5,
    s.mode