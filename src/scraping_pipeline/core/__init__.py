"""The pure core: every rule of the pipeline, with no network, filesystem, clock or randomness.

Anything that needs time or chance takes it as an argument (``now: float``, ``rand: float``), so each
rule is a plain function from inputs to a decision and can be tested exhaustively. The I/O lives in
``fetch/``, ``storage/`` and ``stages/``, which call into this package.
"""
