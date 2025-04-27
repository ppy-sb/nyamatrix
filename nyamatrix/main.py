import coloredlogs
import typer
import logging
from redis import Redis
from sqlalchemy import create_engine
from typing_extensions import Annotated

from nyamatrix import enums, processor, statements

app = typer.Typer()


@app.command(help="Recalculate scores performance points and other affected statistics")
def recalc(
    mysql_uri: Annotated[str, typer.Option("--mysql-uri", "-m", help="Database URI to connect to")] = "mysql+pymysql://localhost:3306",
    redis_uri: Annotated[str, typer.Option("--redis-uri", "-r", help="Redis URI to connect to")] = "redis://localhost:6379",
    beatmap_path: str = typer.Option(..., "--beatmap-path", "-b", help="Path to the beatmaps directory"),
    map_modes: Annotated[
        list[enums.GameMode] | None,
        typer.Option(
            "--game-modes",
            "-gm",
            help="Map modes. (" + ", ".join(f"{mode.name}: {mode.value}" for mode in enums.GameMode) + ")",
        ),
    ] = None,
    score_modes: Annotated[
        list[enums.BanchoPyMode] | None,
        typer.Option(
            "--score-modes",
            "-sm",
            help="Score modes. (" + ", ".join(f"{mode.name}: {mode.value}" for mode in enums.BanchoPyMode) + ")",
        ),
    ] = None,
    score_status: Annotated[
        list[enums.ScoreStatus] | None,
        typer.Option(
            "--score-status",
            "-ss",
            help="Score status (" + ", ".join(f"{status.name}: {status.value}" for status in enums.ScoreStatus) + ")",
        ),
    ] = None,
    map_status: Annotated[
        list[enums.MapStatus] | None,
        typer.Option(
            "--map-status",
            "-ms",
            help="Map status (" + ", ".join(f"{status.name}: {status.value}" for status in enums.MapStatus) + ")",
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


@app.command(help="Full recalculate of scores stats and user statistics")
def reform(
    mysql_uri: Annotated[str, typer.Option("--mysql-uri", "-m", help="Database URI to connect to")] = "mysql+pymysql://localhost:3306",
    redis_uri: Annotated[str, typer.Option("--redis-uri", "-r", help="Redis URI to connect to")] = "redis://localhost:6379",
    table_name: Annotated[list[enums.ReformTarget], typer.Option("--table-name", "-t", help="Target table to reform")] = [
        enums.ReformTarget.UserStats,
        enums.ReformTarget.ScoreStatus,
    ],
    slow_level: Annotated[
        int,
        typer.Option(
            "--slow-level",
            "-sl",
            help="Slow level. (" + ", ".join(f"{level.name}: {level.value}" for level in enums.ReformSlowLevel) + ")",
        ),
    ] = enums.ReformSlowLevel.Normal,
    map_modes: Annotated[
        list[enums.GameMode] | None,
        typer.Option(
            "--game-modes",
            "-gm",
            help="Map modes. (" + ", ".join(f"{mode.name}: {mode.value}" for mode in enums.GameMode) + ")",
        ),
    ] = None,
    score_modes: Annotated[
        list[enums.BanchoPyMode] | None,
        typer.Option(
            "--score-modes",
            "-sm",
            help="Score modes. (" + ", ".join(f"{mode.name}: {mode.value}" for mode in enums.BanchoPyMode) + ")",
        ),
    ] = None,
    score_status: Annotated[
        list[enums.ScoreStatus] | None,
        typer.Option(
            "--score-status",
            "-ss",
            help="Score status (" + ", ".join(f"{status.name}: {status.value}" for status in enums.ScoreStatus) + ")",
        ),
    ] = None,
    map_status: Annotated[
        list[enums.MapStatus] | None,
        typer.Option(
            "--map-status",
            "-ms",
            help="Map status (" + ", ".join(f"{status.name}: {status.value}" for status in enums.MapStatus) + ")",
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

    logging.debug(f"Using database URI: {mysql_uri}")
    logging.debug(f"Using Redis URI: {redis_uri}")

    if statements.test_database_connection(mysql_uri):
        engine = create_engine(mysql_uri, isolation_level="AUTOCOMMIT")
        redis_engine = Redis.from_url(redis_uri, decode_responses=True)
        if enums.ReformTarget.ScoreStatus in table_name:
            processor.qb_process_score_status(
                engine,
                map_modes=map_modes,
                score_modes=score_modes,
                score_statuses=score_status,
                map_statuses=map_status,
                update_failed_scores=slow_level >= enums.ReformSlowLevel.Slowest,
            )
        if enums.ReformTarget.UserStats in table_name:
            processor.qb_process_user_statistics(
                engine,
                redis_engine,
                score_modes=score_modes,
                calc_pp=slow_level >= enums.ReformSlowLevel.Normal,
                slow_statistics=slow_level >= enums.ReformSlowLevel.Slow,
                very_slow_statistics=slow_level >= enums.ReformSlowLevel.Slowest,
            )
        logging.info("Recalculation completed successfully")
    else:
        logging.error("Recalculation failed: Unable to connect to the database")


def main():
    app()


if __name__ == "__main__":
    main()
