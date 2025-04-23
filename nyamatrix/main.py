from typing_extensions import Annotated
import coloredlogs
import typer
import logging
from sqlalchemy import create_engine
from redis import Redis

from nyamatrix import processor, statements, bancho_py

app = typer.Typer()


@app.command()
def recalc(
    mysql_uri: Annotated[str, typer.Option("--mysql-uri", "-m", help="Database URI to connect to")] = "mysql://localhost:3306",
    redis_uri: Annotated[str, typer.Option("--redis-uri", "-r", help="Redis URI to connect to")] = "redis://localhost:6379",
    beatmap_path: str = typer.Option(..., "--beatmap-path", "-b", help="Path to the beatmaps directory"),
    map_modes: Annotated[
        list[bancho_py.GameMode] | None,
        typer.Option(
            "--game-modes",
            "-gm",
            help="Map modes. (" + ", ".join(f"{mode.name}: {mode.value}" for mode in bancho_py.GameMode) + ")",
        ),
    ] = None,
    score_modes: Annotated[
        list[bancho_py.BanchoPyMode] | None,
        typer.Option(
            "--score-modes",
            "-sm",
            help="Score modes. (" + ", ".join(f"{mode.name}: {mode.value}" for mode in bancho_py.BanchoPyMode) + ")",
        ),
    ] = None,
    score_status: Annotated[
        list[bancho_py.ScoreStatus] | None,
        typer.Option(
            "--score-status",
            "-ss",
            help="Score status (" + ", ".join(f"{status.name}: {status.value}" for status in bancho_py.ScoreStatus) + ")",
        ),
    ] = None,
    map_status: Annotated[
        list[bancho_py.MapStatus] | None,
        typer.Option(
            "--map-status",
            "-ms",
            help="Map status (" + ", ".join(f"{status.name}: {status.value}" for status in bancho_py.MapStatus) + ")",
        ),
    ] = None,
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    # Set up logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    logging.basicConfig(level=numeric_level, format="%(asctime)s - %(levelname)s - %(message)s")
    coloredlogs.install(level="DEBUG")

    logging.info(f"Starting recalculation for game modes: {score_modes}")
    logging.debug(f"Using database URI: {mysql_uri}")
    logging.debug(f"Using Redis URI: {redis_uri}")

    if statements.test_database_connection(mysql_uri):
        engine = create_engine(mysql_uri, isolation_level="AUTOCOMMIT")
        redis_engine = Redis.from_url(redis_uri, decode_responses=True)
        processor.qb_process_scores(
            engine,
            beatmap_path,
            map_modes=map_modes,
            score_modes=score_modes,
            score_statuses=score_status,
            map_statuses=map_status,
        )
        processor.qb_process_score_status(
            engine,
            map_modes=map_modes,
            score_modes=score_modes,
            score_statuses=score_status,
            map_statuses=map_status,
        )
        processor.qb_process_user_statistics(
            engine,
            redis_engine,
            score_modes=score_modes,
        )
        logging.info("Recalculation completed successfully")
    else:
        logging.error("Recalculation failed: Unable to connect to the database")


@app.command()
def score_statuses(
    mysql_uri: Annotated[str, typer.Option("--mysql-uri", "-m", help="Database URI to connect to")] = "mysql://localhost:3306",
    redis_uri: Annotated[str, typer.Option("--redis-uri", "-r", help="Redis URI to connect to")] = "redis://localhost:6379",
    map_modes: Annotated[
        list[bancho_py.GameMode] | None,
        typer.Option(
            "--game-modes",
            "-gm",
            help="Map modes. (" + ", ".join(f"{mode.name}: {mode.value}" for mode in bancho_py.GameMode) + ")",
        ),
    ] = None,
    score_modes: Annotated[
        list[bancho_py.BanchoPyMode] | None,
        typer.Option(
            "--score-modes",
            "-sm",
            help="Score modes. (" + ", ".join(f"{mode.name}: {mode.value}" for mode in bancho_py.BanchoPyMode) + ")",
        ),
    ] = None,
    score_status: Annotated[
        list[bancho_py.ScoreStatus] | None,
        typer.Option(
            "--score-status",
            "-ss",
            help="Score status (" + ", ".join(f"{status.name}: {status.value}" for status in bancho_py.ScoreStatus) + ")",
        ),
    ] = None,
    map_status: Annotated[
        list[bancho_py.MapStatus] | None,
        typer.Option(
            "--map-status",
            "-ms",
            help="Map status (" + ", ".join(f"{status.name}: {status.value}" for status in bancho_py.MapStatus) + ")",
        ),
    ] = None,
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    # Set up logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    logging.basicConfig(level=numeric_level, format="%(asctime)s - %(levelname)s - %(message)s")
    coloredlogs.install(level="DEBUG")

    logging.info(f"Recalculating score statuses...")
    logging.debug(f"Using database URI: {mysql_uri}")
    logging.debug(f"Using Redis URI: {redis_uri}")

    if statements.test_database_connection(mysql_uri):
        engine = create_engine(mysql_uri, isolation_level="AUTOCOMMIT")
        processor.qb_process_score_status(
            engine,
            map_modes=map_modes,
            score_modes=score_modes,
            score_statuses=score_status,
            map_statuses=map_status,
        )
        logging.info("Done.")
    else:
        logging.error("Errored.")


@app.command()
def user_statistics(
    mysql_uri: Annotated[str, typer.Option("--mysql-uri", "-m", help="Database URI to connect to")] = "mysql://localhost:3306",
    redis_uri: Annotated[str, typer.Option("--redis-uri", "-r", help="Redis URI to connect to")] = "redis://localhost:6379",
    score_modes: Annotated[
        list[bancho_py.BanchoPyMode] | None,
        typer.Option(
            "--score-modes",
            "-sm",
            help="Score modes. (" + ", ".join(f"{mode.name}: {mode.value}" for mode in bancho_py.BanchoPyMode) + ")",
        ),
    ] = None,
    calc_pp: Annotated[bool, typer.Option("--pp", "-p", help="recalc pp and pp acc")] = True,
    slow_statistics: Annotated[bool, typer.Option("--slow", "-s", help="recalc slow statistics")] = True,
    very_slow_statistics: Annotated[bool, typer.Option("--very-slow", "-ss", help="recalc very slow statistics")] = True,
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    # Set up logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    logging.basicConfig(level=numeric_level, format="%(asctime)s - %(levelname)s - %(message)s")
    coloredlogs.install(level="DEBUG")

    logging.info(f"Recalculating score statuses...")
    logging.debug(f"Using database URI: {mysql_uri}")
    logging.debug(f"Using Redis URI: {redis_uri}")

    if statements.test_database_connection(mysql_uri):
        engine = create_engine(mysql_uri, isolation_level="AUTOCOMMIT")
        redis_engine = Redis.from_url(redis_uri, decode_responses=True)
        processor.qb_process_user_statistics(
            engine,
            redis_engine,
            score_modes=score_modes,
            calc_pp=calc_pp,
            slow_statistics=slow_statistics,
            very_slow_statistics=very_slow_statistics,
        )
        logging.info("Done.")
    else:
        logging.error("Errored.")


def main():
    app()


if __name__ == "__main__":
    main()
