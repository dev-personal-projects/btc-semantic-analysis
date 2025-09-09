import os
import click
from datetime import datetime
from .pipelines.ingest_pipeline import run_simple_analysis
from .config.config import get_settings

@click.command()
@click.option(
    "--days-back",
    default=1,
    show_default=True,
    help="How many days back to analyze."
)
@click.option(
    "--env",
    default=None,
    help="Environment config to use (e.g., development, production)."
)
def main(days_back, env):
    """
    Run simple sentiment analysis for the specified time period.
    """
    if env:
        os.environ["ENVIRONMENT"] = env
        get_settings.cache_clear()
        click.echo(f"Using configuration environment: {env}")

    click.echo(f"Starting sentiment analysis for {days_back} day(s)...")
    start_time = datetime.utcnow()
    daily = run_simple_analysis(days_back=days_back)
    end_time = datetime.utcnow()

    click.echo(f"Analysis completed in {end_time - start_time}")
    click.echo(f"Generated {len(daily)} daily sentiment records.")

if __name__ == "__main__":
    main()