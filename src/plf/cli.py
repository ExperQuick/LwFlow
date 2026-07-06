"""
plf - command line interface for PyLabFlow.

Lets users manage experiment pipelines (PPLs) from the terminal,
without writing Python scripts or opening a Jupyter notebook.
"""

import json
import os
import sys

import click

from plf.lab import create_project, lab_setup
from plf.experiment import (
    PipeLine,
    archive_ppl,
    compare_ppl_configs,
    delete_ppl,
    get_ppl_history,
    get_ppl_status,
    get_ppls,
    get_runnings,
)


def _load_lab(settings):
    """
    Load the active lab. If `settings` is given, use that file.
    Otherwise walk up from cwd looking for a settings.json (like git finds .git/).
    """
    if settings:
        lab_setup(settings)
        return

    path = os.getcwd()
    while True:
        candidate = os.path.join(path, "settings.json")
        if os.path.exists(candidate):
            lab_setup(candidate)
            return
        parent = os.path.dirname(path)
        if parent == path:
            break
        path = parent

    click.echo("Error: No settings.json found in current or parent directories.", err=True)
    click.echo("Tip: run `plf init --config <settings.json>` first, or use --settings.", err=True)
    sys.exit(1)


@click.group()
@click.option("--settings", default=None, help="Path to settings.json (overrides auto-detect).")
@click.pass_context
def main(ctx, settings):
    """plf - PyLabFlow CLI for managing experiment pipelines."""
    ctx.ensure_object(dict)
    ctx.obj["settings"] = settings


@main.command()
@click.pass_context
def status(ctx):
    """Show status of all PPLs as a table."""
    _load_lab(ctx.obj["settings"])
    df = get_ppl_status()
    if df is None or df.empty:
        click.echo("No pipelines found.")
        return
    if "pplid" not in df.columns:
        df = df.reset_index().rename(columns={"index": "pplid"})
    click.echo(df.to_string(index=False))


@main.command(name="list")
@click.pass_context
def list_ppls(ctx):
    """List all active PPL IDs."""
    _load_lab(ctx.obj["settings"])
    ppls = get_ppls()
    if not ppls:
        click.echo("No active pipelines.")
        return
    for ppl in ppls:
        click.echo(ppl)


@main.command()
@click.argument("pplid_a")
@click.argument("pplid_b")
@click.pass_context
def compare(ctx, pplid_a, pplid_b):
    """Compare configs of two pipelines and show differences."""
    _load_lab(ctx.obj["settings"])
    try:
        result = compare_ppl_configs(pplid_a, pplid_b)
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    if result["identical"]:
        click.echo(f"Configs for '{pplid_a}' and '{pplid_b}' are identical.")
        return

    click.echo(f"Differences between '{pplid_a}' and '{pplid_b}':")
    for path, values in sorted(result["differences"].items()):
        click.echo(f"  {path}:")
        click.echo(f"    {pplid_a}: {values['left']!r}")
        click.echo(f"    {pplid_b}: {values['right']!r}")


@main.command(name="log")
@click.argument("pplid")
@click.pass_context
def show_log(ctx, pplid):
    """Show run history for a pipeline."""
    _load_lab(ctx.obj["settings"])
    try:
        df = get_ppl_history(pplid)
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    if df.empty:
        click.echo(f"No run history found for '{pplid}'.")
        return

    click.echo(df.to_string(index=False))


@main.command()
@click.argument("pplid")
@click.pass_context
def run(ctx, pplid):
    """Run a specific pipeline by PPLID."""
    _load_lab(ctx.obj["settings"])
    P = PipeLine()
    P.load(pplid)
    P.prepare()
    P.run()
    click.echo(f"Finished: {pplid}")


@main.command()
@click.argument("pplid")
@click.pass_context
def archive(ctx, pplid):
    """Archive a pipeline."""
    _load_lab(ctx.obj["settings"])
    archive_ppl([pplid])
    click.echo(f"Archived: {pplid}")


@main.command()
@click.argument("pplid")
@click.pass_context
def delete(ctx, pplid):
    """Delete a pipeline from the archive."""
    _load_lab(ctx.obj["settings"])
    delete_ppl([pplid])
    click.echo(f"Deleted: {pplid}")


@main.command()
@click.pass_context
def stop(ctx):
    """Gracefully stop any currently running pipeline(s)."""
    _load_lab(ctx.obj["settings"])
    df = get_runnings()
    if df is None or df.empty:
        click.echo("No pipeline is currently running.")
        return
    for pplid in df["pplid"].unique():
        P = PipeLine()
        P.load(pplid)
        P.stop_running()


@main.command()
@click.argument("pplid")
@click.option("--format", "fmt", type=click.Choice(["yaml", "json"]), default="json", show_default=True)
@click.option("--out", default=".", show_default=True, help="Output directory.")
@click.pass_context
def export(ctx, pplid, fmt, out):
    """Export a pipeline config to YAML or JSON."""
    _load_lab(ctx.obj["settings"])
    P = PipeLine()
    P.load(pplid)
    config = P.cnfg
    os.makedirs(out, exist_ok=True)
    outfile = os.path.join(out, f"{pplid}.{fmt}")
    if fmt == "json":
        with open(outfile, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
    else:  # yaml
        try:
            import yaml
        except ImportError:
            click.echo("PyYAML is not installed. Run: pip install pyyaml", err=True)
            sys.exit(1)
        with open(outfile, "w", encoding="utf-8") as f:
            yaml.dump(config, f)
    click.echo(f"Exported to {outfile}")


@main.command(name="init")
@click.option("--config", required=True, help="Path to a settings JSON file.")
def init(config):
    """Set up a lab environment from a settings file."""
    with open(config, encoding="utf-8") as f:
        settings = json.load(f)
    settings_path = create_project(settings)
    click.echo(f"Lab initialized. Settings at: {settings_path}")


if __name__ == "__main__":
    main()
