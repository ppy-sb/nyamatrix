import json
import logging
from tqdm import tqdm
from pathlib import Path
from redis import Redis
from sqlalchemy import Engine, text
from rosu_pp_py import Beatmap, GameMode, Performance, PerformanceAttributes
from concurrent.futures import ThreadPoolExecutor

from nyamatrix import statements
from nyamatrix.statements import SQL

STATEMENT_GROUP_SCORES = SQL("group_scores")
STATEMENT_COUNT_SCORES = "SELECT MAX(id) FROM scores WHERE status > 0 AND mode IN :modes"
STATEMENT_UPDATE_SCORES = "UPDATE scores SET pp = :pp WHERE id = :id"
STATEMENT_UPDATE_SCORE_STATUS = SQL("update_score_status")
STATEMENT_UPDATE_USER_STATISTICS = SQL("update_user_statistics")
STATEMENT_COUNT_USER_STATISTICS = "SELECT COUNT(*) FROM stats s INNER JOIN users u ON s.id = u.id WHERE s.mode IN :modes"
STATEMENT_FETCH_USER_STATISTICS = "SELECT s.id, s.mode, s.pp, u.country, u.priv FROM stats s INNER JOIN users u ON s.id = u.id WHERE s.mode IN :modes"

map_path = Path.cwd() / "maps"
gm_dict: dict[int, GameMode] = {
    0: GameMode.Osu,
    1: GameMode.Taiko,
    2: GameMode.Catch,
    3: GameMode.Mania,
}


def _process_score(attr_or_map: Beatmap | PerformanceAttributes, array: tuple) -> PerformanceAttributes | None:
    calculator = Performance(
        mods=array[0],
        combo=array[1],
        n_geki=array[2],
        n300=array[3],
        n_katu=array[4],
        n100=array[5],
        n50=array[6],
        misses=array[7],
        lazer=False,
    )
    try:
        attrs = calculator.calculate(attr_or_map)
        return attrs
    except Exception as e:
        logging.error(f"Error calculating performance attributes: {e}")


def _process_group(map_id: int, mode: int, scores: list[tuple], map_path: str, tqdm: tqdm, engine: Engine):
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
                    results_list[i] = (score[0], result_attr.pp)

            with engine.connect() as conn:
                conn.execute(text(STATEMENT_UPDATE_SCORES), [{"pp": result[1], "id": result[0]} for result in results_list])
        tqdm.update(scores_num)
    except Exception as e:
        logging.error(f"Error processing group for map ID {map_id} and mode {mode}: {e}")


def process_scores(engine: Engine, gamemodes: list[int], map_path: str) -> None:
    logging.info("Processing scores (python procedure, progressbar is available).")
    progress_bar = tqdm(total=statements.fetch_count(engine, STATEMENT_COUNT_SCORES, {"modes": tuple(gamemodes)}))
    pool = ThreadPoolExecutor(max_workers=4)
    with engine.connect() as conn:
        connection = conn.execution_options(stream_results=True, max_row_buffer=10000)
        with connection.execute(text(STATEMENT_GROUP_SCORES), {"modes": tuple(gamemodes)}) as result:
            for grouped_scores in result:
                pool.submit(_process_group, grouped_scores[0], grouped_scores[1], json.loads(grouped_scores[2]), map_path, progress_bar, engine)
    pool.shutdown(wait=True)
    progress_bar.close()
    logging.info("Finished processing scores.")


def process_score_status(engine: Engine, gamemodes: list[int]) -> None:
    logging.info("Processing full table scores status (large mysql procedure, waiting mysql to finish).")
    with engine.connect() as conn:
        conn.execute(text(STATEMENT_UPDATE_SCORE_STATUS), {"modes": tuple(gamemodes)})
    logging.info("Finished processing status.")


def process_user_statistics(engine: Engine, redis: Redis, gamemodes: list[int]) -> None:
    logging.info("Processing full table user statistics (large mysql procedure, waiting mysql to finish).")
    with engine.connect() as conn:
        conn.execute(text(STATEMENT_UPDATE_USER_STATISTICS), {"modes": tuple(gamemodes)})
    logging.info("Finished processing user statistics.")

    logging.info("Writing leaderboard to redis (python procedure, progressbar is available).")
    progress_bar = tqdm(total=statements.fetch_count(engine, STATEMENT_COUNT_USER_STATISTICS, {"modes": tuple(gamemodes)}))
    with engine.connect() as conn:
        connection = conn.execution_options(stream_results=True, max_row_buffer=1000)
        with connection.execute(text(STATEMENT_FETCH_USER_STATISTICS), {"modes": tuple(gamemodes)}) as result:
            for row in result:
                if row[4] & 1 << 0:  # unrestricted
                    redis.zadd(f"bancho:leaderboard:{row[1]}", {str(row[0]): row[2]})
                    redis.zadd(f"bancho:leaderboard:{row[1]}:{row[3]}", {str(row[0]): row[2]})
                progress_bar.update(1)
    logging.info("Finished processing user statistics.")
