"""Versioned data contracts: the strict shape every published row must have.

One module per site and major version (``books_v1``, ``quotes_v1``). Each exports its models and a
``CONTRACT_VERSION`` such as ``"books/1.0.0"``, which is stamped into every row's lineage and checked
on validation. The module docstrings say how a version bumps and keep the changelog.
"""

from scraping_pipeline.core.contracts.books_v1 import Book
from scraping_pipeline.core.contracts.quotes_v1 import Author, Quote

type Record = Book | Quote | Author

__all__ = ["Author", "Book", "Quote", "Record"]
