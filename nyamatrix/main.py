import coloredlogs
import typer
import logging
from sqlalchemy import create_engine
from redis import Redis

from nyamatrix import processor, statements

app = typer.Typer()


@app.command()
def recalc(
    mysql_uri: str = typer.Option(..., "--mysql-uri", "-d", help="Database URI to connect to"),
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
        engine = create_engine(mysql_uri)
        redis_engine = Redis.from_url(redis_uri, decode_responses=True)
        processor.process_scores(engine, gamemodes, beatmap_path)
        processor.process_score_status(engine, gamemodes)
        processor.process_user_statistics(engine, redis_engine, gamemodes)
        logging.info("Recalculation completed successfully")
    else:
        logging.error("Recalculation failed: Unable to connect to the database")


def main():
    app()


if __name__ == "__main__":
    main()
