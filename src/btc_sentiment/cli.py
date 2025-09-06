import os
import click
from datetime import datetime
from .pipelines.ingest_pipeline import run_ingest_pipeline
from .config.config import get_settings

@click.command()
@click.option(
    "--days-back",
    default=1,
    show_default=True,
    help="How many days back to fetch data."
)
@click.option(
    "--tweets-per-day",
    default=50,
    show_default=True,
    help="Maximum X/Twitter tweets to fetch per run."
)
@click.option(
    "--messages-per-channel",
    default=50,
    show_default=True,
    help="Max Telegram messages per channel."
)
@click.option(
    "--env",
    default=None,
    help="Environment config to use (e.g., development, production)."
)
def main(days_back, tweets_per_day, messages_per_channel, env):
    """
    Run the ingest pipeline to fetch data, analyze sentiment, aggregate daily scores,
    save results, and plot trends.
    """
    if env:
        os.environ["ENVIRONMENT"] = env
        get_settings.cache_clear()
        click.echo(f"Using configuration environment: {env}")

    click.echo("Starting BTC sentiment ingest pipeline...")
    start_time = datetime.utcnow()
    daily = run_ingest_pipeline(
        days_back=days_back,
        tweets_per_day=tweets_per_day,
        messages_per_channel=messages_per_channel
    )
    end_time = datetime.utcnow()

    click.echo(f"Pipeline completed in {end_time - start_time}")
    click.echo(f"Processed {len(daily)} daily sentiment records.")

if __name__ == "__main__":
    main()