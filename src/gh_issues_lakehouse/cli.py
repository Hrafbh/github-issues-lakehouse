import argparse

def main():
    parser = argparse.ArgumentParser(prog="gh-issues-lakehouse")

    parser.add_argument(
        "command",
        choices=["ingest", "silver", "gold"],
        help="Which step to run"
    )

    args = parser.parse_args()

    print(f"Command selected: {args.command}")

if __name__ == "__main__":
    main()
