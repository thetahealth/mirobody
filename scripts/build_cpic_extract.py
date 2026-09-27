"""Build a deterministic CPIC runtime extract from a pinned public SQL dump."""

from mirobody.translate.cpic_extract import TABLE_COLUMNS, extract, main, write_extract

__all__ = ["TABLE_COLUMNS", "extract", "write_extract", "main"]


if __name__ == "__main__":
    main()
