"""Shared, dependency-free building blocks used by every component.

Nothing in this package may import pyspark, so the lightweight producer and
dashboard images can use it without pulling in a JVM.
"""
