"""Tests unitaires du calcul de drift (PSI) utilisé pour déclencher le réentraînement."""

import numpy as np
import pytest

from ml.features import bin_shares, psi


def test_bin_shares_somme_a_un_et_bornes():
    edges = [0, 10, 20, 30]
    shares = bin_shares([-5, 5, 15, 25, 99, np.nan], edges)
    assert shares.sum() == pytest.approx(1.0)
    # -5 tombe dans la première classe, 99 dans la dernière, NaN est ignoré
    assert list(shares) == pytest.approx([0.4, 0.2, 0.4])


def test_psi_nul_si_distributions_identiques():
    shares = [0.25, 0.25, 0.25, 0.25]
    assert psi(shares, shares) == pytest.approx(0.0)


def test_psi_eleve_si_distribution_deplacee():
    reference = [0.25, 0.25, 0.25, 0.25]
    actuelle = [0.0, 0.0, 0.1, 0.9]
    assert psi(reference, actuelle) > 0.2  # seuil de réentraînement


def test_psi_positif_et_robuste_aux_classes_vides():
    value = psi([0.5, 0.5, 0.0], [0.0, 0.5, 0.5])
    assert np.isfinite(value) and value > 0
