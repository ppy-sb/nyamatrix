import json
import logging
import math
from typing import Optional
from tqdm import tqdm
from pathlib import Path
from redis import Redis
from sqlalchemy import Engine, text
from rosu_pp_py import Beatmap, GameMode, Performance, PerformanceAttributes
from concurrent.futures import ThreadPoolExecutor

from nyamatrix import statements
from nyamatrix.statements import SQL

from nyamatrix import bancho_py

from nyamatrix.qb.group_scores import query as qb_group_scores, count as qb_count_scores
from nyamatrix.qb.update_score_status import query as qb_update_score_status
from nyamatrix.qb.update_user_statistics_use_status import query as qb_update_user_statistics

STATEMENT_GROUP_SCORES = SQL("group_scores")
STATEMENT_COUNT_SCORES = "SELECT COUNT(*) FROM scores WHERE status > 0 AND mode IN :modes"
STATEMENT_UPDATE_SCORES = "UPDATE scores SET pp = :pp WHERE id = :id"
STATEMENT_UPDATE_SCORE_STATUS = SQL("update_score_status")
STATEMENT_UPDATE_USER_STATISTICS = SQL("update_user_statistics")
STATEMENT_COUNT_USER_STATISTICS = (
    "SELECT COUNT(*) FROM stats s INNER JOIN users u ON s.id = u.id WHERE s.mode IN :modes"
)
STATEMENT_FETCH_USER_STATISTICS = (
    "SELECT s.id, s.mode, s.pp, u.country, u.priv FROM stats s INNER JOIN users u ON s.id = u.id WHERE s.mode IN :modes"
)

map_path = Path.cwd() / "maps"
gm_dict: dict[int, GameMode] = {
    0: GameMode.Osu,
    1: GameMode.Taiko,
    2: GameMode.Catch,
    3: GameMode.Mania,
}


def _process_score(
    attr_or_map: Beatmap | PerformanceAttributes, array: tuple[int, int, int, int, int, int, int, int]
) -> PerformanceAttributes | None:
    mods, combo, n_geki, n300, n_katu, n100, n50, misses = array
    calculator = Performance(
        mods=mods,
        combo=combo,
        n_geki=n_geki,
        n300=n300,
        n_katu=n_katu,
        n100=n100,
        n50=n50,
        misses=misses,
        lazer=False,
    )
    try:
        attrs = calculator.calculate(attr_or_map)
        return attrs
    except Exception as e:
        logging.error(f"Error calculating performance attributes: {e}")


def _process_group(
    map_id: int,
    mode: int,
    scores: list[tuple[int, int, int, int, int, int, int, int, int]],
    map_path: str,
    tqdm: tqdm,
    engine: Engine,
):
    try:
        scores_num = len(scores)
        beatmap_path = Path(map_path) / f"{map_id}.osu"
        if beatmap_path.exists():
            beatmap = Beatmap(path=str(beatmap_path))
            beatmap.convert(gm_dict[mode % 4], None)

            results_list: list[tuple[int, float]] = [None] * scores_num  # type: ignore
            attr_buffer: dict[int, PerformanceAttributes] = {}

            for i, score in enumerate(scores):
                attr_or_map = attr_buffer.get(score[1], beatmap)
                if result_attr := _process_score(attr_or_map, score[1:]):
                    if isinstance(attr_or_map, Beatmap):
                        attr_buffer[score[1]] = result_attr
                    pp_value = result_attr.pp
                    if math.isnan(pp_value) or math.isinf(pp_value) or pp_value > 9999:
                        pp_value = 0.0
                    results_list[i] = (score[0], pp_value)

            with engine.connect() as conn:
                conn.execute(
                    text(STATEMENT_UPDATE_SCORES), [{"pp": result[1], "id": result[0]} for result in results_list]
                )
                conn.commit()
        tqdm.update(scores_num)
    except Exception as e:
        logging.error(f"Error processing group for map ID {map_id} and mode {mode}: {e}")


def qb_process_scores(
    engine: Engine,
    map_path: str,
    *,
    score_modes: Optional[list[bancho_py.BanchoPyMode]] = None,
    map_modes: Optional[list[bancho_py.GameMode]] = None,
    score_statuses: Optional[list[bancho_py.ScoreStatus]] = None,
    map_statuses: Optional[list[bancho_py.MapStatus]] = None,
    user_ids: Optional[list[int]] = None,
    time_after: Optional[int] = None,
    time_before: Optional[int] = None,
) -> None:
    logging.info("Processing scores.")
    count, count_params = qb_count_scores(
        score_modes=[int(mode.value) for mode in score_modes] if score_modes else None,
        map_modes=[int(mode.value) for mode in map_modes] if map_modes else None,
        score_statuses=[int(status.value) for status in score_statuses] if score_statuses else None,
        map_statuses=[int(status.value) for status in map_statuses] if map_statuses else None,
        user_ids=user_ids,
        time_after=time_after,
        time_before=time_before,
    )
    progress_bar = tqdm(total=statements.fetch_count(engine, count, count_params))
    pool = ThreadPoolExecutor(max_workers=4)
    with engine.connect() as conn:
        connection = conn.execution_options(stream_results=True, max_row_buffer=10000)
        query, query_params = qb_group_scores(
            score_modes=[int(mode.value) for mode in score_modes] if score_modes else None,
            map_modes=[int(mode.value) for mode in map_modes] if map_modes else None,
            score_statuses=[int(status.value) for status in score_statuses] if score_statuses else None,
            map_statuses=[int(status.value) for status in map_statuses] if map_statuses else None,
            user_ids=user_ids,
            time_after=time_after,
            time_before=time_before,
        )
        with connection.execute(text(query), query_params) as result:
            for v in result:
                beatmap_id, score_mode, scores = v
                pool.submit(
                    _process_group,
                    beatmap_id,
                    score_mode,
                    json.loads(scores),
                    map_path,
                    progress_bar,
                    engine,
                )
    pool.shutdown(wait=True)
    progress_bar.close()
    logging.info("Finished processing scores.")


def process_scores(engine: Engine, gamemodes: list[int], map_path: str) -> None:
    logging.info("Processing scores.")
    progress_bar = tqdm(total=statements.fetch_count(engine, STATEMENT_COUNT_SCORES, {"modes": tuple(gamemodes)}))
    pool = ThreadPoolExecutor(max_workers=4)
    with engine.connect() as conn:
        connection = conn.execution_options(stream_results=True, max_row_buffer=10000)
        with connection.execute(text(STATEMENT_GROUP_SCORES), {"modes": tuple(gamemodes)}) as result:
            for grouped_scores in result:
                pool.submit(
                    _process_group,
                    grouped_scores[0],
                    grouped_scores[1],
                    json.loads(grouped_scores[2]),
                    map_path,
                    progress_bar,
                    engine,
                )
    pool.shutdown(wait=True)
    progress_bar.close()
    logging.info("Finished processing scores.")


def qb_process_score_status(
    engine: Engine,
    *,
    score_modes: Optional[list[bancho_py.BanchoPyMode]] = None,
    map_modes: Optional[list[bancho_py.GameMode]] = None,
    score_statuses: Optional[list[bancho_py.ScoreStatus]] = None,
    map_statuses: Optional[list[bancho_py.MapStatus]] = None,
    user_ids: Optional[list[int]] = None,
    time_after: Optional[int] = None,
    time_before: Optional[int] = None,
    update_failed_scores: Optional[bool] = False,
) -> None:
    logging.info("Processing full table scores status (waiting for mysql).")
    with engine.connect() as conn:
        q, b = qb_update_score_status(
            score_modes=[int(mode.value) for mode in score_modes] if score_modes else None,
            map_modes=[int(mode.value) for mode in map_modes] if map_modes else None,
            score_statuses=[int(status.value) for status in score_statuses] if score_statuses else None,
            map_statuses=[int(status.value) for status in map_statuses] if map_statuses else None,
            user_ids=user_ids,
            time_after=time_after,
            time_before=time_before,
            update_failed_scores=update_failed_scores,
        )
        conn.execute(text(q), b)
        conn.commit()
    logging.info("Finished processing status.")


def process_score_status(engine: Engine, gamemodes: list[int]) -> None:
    logging.info("Processing full table scores status (waiting for mysql).")
    with engine.connect() as conn:
        conn.execute(text(STATEMENT_UPDATE_SCORE_STATUS), {"modes": tuple(gamemodes)})
        conn.commit()
    logging.info("Finished processing status.")


def qb_process_user_statistics(
    engine: Engine,
    redis: Redis,
    *,
    score_modes: Optional[list[bancho_py.BanchoPyMode]] = None,
    calc_pp: Optional[bool] = None,
    slow_statistics: Optional[bool] = None,
    very_slow_statistics: Optional[bool] = None,
    user_ids: Optional[list[int]] = None,  # TODO
) -> None:
    logging.info("Processing full table user statistics (waiting for mysql).")
    with engine.connect() as conn:
        q, b = qb_update_user_statistics(
            modes=[int(mode.value) for mode in score_modes] if score_modes else None,
            calc_pp=calc_pp,
            slow_statistics=slow_statistics,
            very_slow_statistics=very_slow_statistics,
        )
        if q is None:
            return
        conn.execute(text(q), b)
        conn.commit()
    logging.info("Writing leaderboard to redis.")
    progress_bar = tqdm(
        total=statements.fetch_count(
            engine,
            STATEMENT_COUNT_USER_STATISTICS,
            {"modes": [int(mode.value) for mode in score_modes] if score_modes else [0, 1, 2, 3, 4, 5, 6, 8]},
        )
    )
    with engine.connect() as conn:
        connection = conn.execution_options(stream_results=True, max_row_buffer=1000)
        with connection.execute(
            text(STATEMENT_FETCH_USER_STATISTICS),
            {"modes": [int(mode.value) for mode in score_modes] if score_modes else [0, 1, 2, 3, 4, 5, 6, 8]},
        ) as result:
            for row in result:
                if row[4] & 1 << 0:  # unrestricted
                    redis.zadd(f"bancho:leaderboard:{row[1]}", {str(row[0]): row[2]})
                    redis.zadd(f"bancho:leaderboard:{row[1]}:{row[3]}", {str(row[0]): row[2]})
                progress_bar.update(1)
    logging.info("Finished processing user statistics.")


def process_user_statistics(engine: Engine, redis: Redis, gamemodes: list[int]) -> None:
    logging.info("Processing full table user statistics (waiting for mysql).")
    with engine.connect() as conn:
        conn.execute(text(STATEMENT_UPDATE_USER_STATISTICS), {"modes": tuple(gamemodes)})
        conn.commit()
    logging.info("Finished processing user statistics.")

    logging.info("Writing leaderboard to redis.")
    progress_bar = tqdm(
        total=statements.fetch_count(engine, STATEMENT_COUNT_USER_STATISTICS, {"modes": tuple(gamemodes)})
    )
    with engine.connect() as conn:
        connection = conn.execution_options(stream_results=True, max_row_buffer=1000)
        with connection.execute(text(STATEMENT_FETCH_USER_STATISTICS), {"modes": tuple(gamemodes)}) as result:
            for row in result:
                if row[4] & 1 << 0:  # unrestricted
                    redis.zadd(f"bancho:leaderboard:{row[1]}", {str(row[0]): row[2]})
                    redis.zadd(f"bancho:leaderboard:{row[1]}:{row[3]}", {str(row[0]): row[2]})
                progress_bar.update(1)
    logging.info("Finished processing user statistics.")
