"""Ensure classification service directory is on sys.path for local imports."""
import sys, os

_svc = os.path.join(os.path.dirname(__file__), "..", "..", "services", "classification-deepfaune")
_svc = os.path.abspath(_svc)
if _svc not in sys.path:
    sys.path.insert(0, _svc)

# SpeciesNet is the deployed classifier and owns pipeline recovery semantics.
_speciesnet = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "services", "classification-speciesnet"))
if _speciesnet not in sys.path:
    sys.path.insert(0, _speciesnet)
