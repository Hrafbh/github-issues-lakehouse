import argparse

def main():
    parser = argparse.ArgumentParser(prog="gh-issues-lakehouse")

    parser.add_argument(
        "command",
        choices=["ingest", "silver", "gold"],
        help="Which step to run"
    )

    args = parser.parse_args()

    if args.command == "ingest":
        from gh_issues_lakehouse.ingest import run_ingest
        run_ingest()
    else:
        print(f"{args.command} not implemented yet (coming in next commits).")

    print(f"Command selected: {args.command}")

if __name__ == "__main__":
    main()
