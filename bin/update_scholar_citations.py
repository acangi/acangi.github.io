#!/usr/bin/env python

import os
import sys
from datetime import datetime


def env_truthy(name: str) -> bool:
    """Return True when the environment variable is set to a truthy value."""
    value = os.getenv(name, "")
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_scholar_user_id() -> str:
    """Load the Google Scholar user ID from the configuration file."""
    import yaml

    config_file = "_data/socials.yml"
    if not os.path.exists(config_file):
        raise FileNotFoundError(
            f"Configuration file {config_file} not found. Please ensure it contains your Google Scholar user ID."
        )
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        scholar_user_id = config.get("scholar_userid")
        if not scholar_user_id:
            raise ValueError(
                "No 'scholar_userid' found in the configuration file. Please add it to _data/socials.yml."
            )
        return scholar_user_id
    except yaml.YAMLError as e:
        raise ValueError(
            f"Error parsing YAML file {config_file}: {e}. Please check its YAML syntax."
        ) from e


OUTPUT_FILE: str = "_data/citations.yml"


def should_skip_fetch() -> bool:
    """Return True when the citations fetch should be skipped."""
    return env_truthy("SCHOLAR_SKIP_FETCH")


def allow_failure() -> bool:
    """Return True when fetch failures should not stop the workflow."""
    return env_truthy("SCHOLAR_ALLOW_FAILURE")


def fail_or_warn(message: str) -> None:
    """Exit on failure unless SCHOLAR_ALLOW_FAILURE is enabled."""
    if allow_failure():
        print(f"Warning: {message} Keeping existing citation cache.")
        return
    print(message)
    print("Tip: set SCHOLAR_ALLOW_FAILURE=1 to continue without failing.")
    sys.exit(1)


def get_scholar_citations() -> None:
    """Fetch and update Google Scholar citation data."""
    if should_skip_fetch():
        print("Skipping Google Scholar fetch because SCHOLAR_SKIP_FETCH is set.")
        return

    # Import lazily so SCHOLAR_ALLOW_FAILURE also covers unavailable or
    # incompatible dependencies in CI.
    import yaml
    from scholarly import scholarly

    scholar_user_id = load_scholar_user_id()
    print(f"Fetching citations for Google Scholar ID: {scholar_user_id}")
    today = datetime.now().strftime("%Y-%m-%d")
    existing_data = None

    # Check if the output file was already updated today
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r") as f:
                existing_data = yaml.safe_load(f)
            if (
                existing_data
                and "metadata" in existing_data
                and "last_updated" in existing_data["metadata"]
            ):
                print(f"Last updated on: {existing_data['metadata']['last_updated']}")
                if existing_data["metadata"]["last_updated"] == today:
                    print("Citations data is already up-to-date. Skipping fetch.")
                    return
        except Exception as e:
            print(
                f"Warning: Could not read existing citation data from {OUTPUT_FILE}: {e}. The file may be missing or corrupted."
            )

    citation_data = {"metadata": {"last_updated": today}, "papers": {}}

    scholarly.set_timeout(15)
    scholarly.set_retries(3)
    try:
        author = scholarly.search_author_id(scholar_user_id)
        author_data = scholarly.fill(author)
    except Exception as e:
        fail_or_warn(
            f"Error fetching author data from Google Scholar for user ID '{scholar_user_id}': {e}. Please check your internet connection and Scholar user ID."
        )
        return

    if not author_data:
        fail_or_warn(
            f"Could not fetch author data for user ID '{scholar_user_id}'. Please verify the Scholar user ID and try again."
        )
        return

    if "publications" not in author_data:
        fail_or_warn(
            f"No publications found in author data for user ID '{scholar_user_id}'."
        )
        return

    for pub in author_data["publications"]:
        try:
            pub_id = pub.get("pub_id") or pub.get("author_pub_id")
            if not pub_id:
                print(
                    f"Warning: No ID found for publication: {pub.get('bib', {}).get('title', 'Unknown')}. This publication will be skipped."
                )
                continue

            title = pub.get("bib", {}).get("title", "Unknown Title")
            year = pub.get("bib", {}).get("pub_year", "Unknown Year")
            citations = pub.get("num_citations", 0)

            print(f"Found: {title} ({year}) - Citations: {citations}")

            citation_data["papers"][pub_id] = {
                "title": title,
                "year": year,
                "citations": citations,
            }
        except Exception as e:
            print(
                f"Error processing publication '{pub.get('bib', {}).get('title', 'Unknown')}': {e}. This publication will be skipped."
            )

    # Compare new data with existing data
    if existing_data and existing_data.get("papers") == citation_data["papers"]:
        print("No changes in citation data. Skipping file update.")
        return

    with open(OUTPUT_FILE, "w") as f:
        yaml.dump(citation_data, f, width=1000, sort_keys=True)
    print(f"Citation data saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    try:
        get_scholar_citations()
    except Exception as e:
        fail_or_warn(f"Unexpected error while updating the Google Scholar cache: {e}")
