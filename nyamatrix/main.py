from typing_extensions import Annotated
import coloredlogs
import typer
import logging
from sqlalchemy import create_engine
from redis import Redis

from nyamatrix import processor, statements
import bancho_py

app = typer.Typer()


@app.command()
def recalc(
    mysql_uri: str = typer.Option(..., "--mysql-uri", "-m", help="Database URI to connect to"),
    redis_uri: str = typer.Option("redis://localhost:6379", "--redis-uri", "-r", help="Redis URI to connect to"),
    beatmap_path: str = typer.Option(..., "--beatmap-path", "-b", help="Path to the beatmaps directory"),
    gamemodes: list[int] = typer.Option(
        [0, 1, 2, 3, 4, 5, 6, 8],
        "--gamemodes",
        "-g",
        help="Game modes to recalculate (0: osu, 1: taiko, 2: catch, 3: mania, 4-6: for relax, 8: for autopilot)",
    ),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
):
    # Set up logging
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    logging.basicConfig(level=numeric_level, format="%(asctime)s - %(levelname)s - %(message)s")
    coloredlogs.install(level="DEBUG")

    logging.info(f"Starting recalculation for game modes: {gamemodes}")
    logging.debug(f"Using database URI: {mysql_uri}")
    logging.debug(f"Using Redis URI: {redis_uri}")

    if statements.test_database_connection(mysql_uri):
        engine = create_engine(mysql_uri, isolation_level="AUTOCOMMIT")
        redis_engine = Redis.from_url(redis_uri, decode_responses=True)
        processor.process_scores(engine, gamemodes, beatmap_path)
        processor.process_score_status(engine, gamemodes)
        processor.process_user_statistics(engine, redis_engine, gamemodes)
        logging.info("Recalculation completed successfully")
    else:
        logging.error("Recalculation failed: Unable to connect to the database")


@app.command()
def score_statuses(
    mysql_uri: Annotated[
        str, typer.Option("--mysql-uri", "-m", help="Database URI to connect to")
    ] = "mysql://localhost:3306",
    redis_uri: Annotated[
        str, typer.Option("--redis-uri", "-r", help="Redis URI to connect to")
    ] = "redis://localhost:6379",
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
            help="Score status ("
            + ", ".join(f"{status.name}: {status.value}" for status in bancho_py.ScoreStatus)
            + ")",
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
):
    print(f"mysql_uri: {mysql_uri}")
    print(f"redis_uri: {redis_uri}")
    print(f"map_modes: {map_modes}")
    print(f"score_modes: {score_modes}")
    print(f"score_status: {score_status}")
    print(f"map_status: {map_status}")


def main():
    app()


if __name__ == "__main__":
    main()
