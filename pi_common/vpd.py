"""Vapour pressure deficit.

The single number this project is named after, and it was written out twice -
identically - in dht22.py and C5A.py. Two copies of a formula is two chances
for a sensor type to compute something subtly different from the other one and
for nobody to notice, because both numbers look plausible.
"""

import math


def vpd_kpa(temperature, humidity):
    """Vapour pressure deficit in kPa.

    Tetens: saturation vapour pressure at `temperature` (degrees C), scaled by
    how far `humidity` (0-100 %RH) is below saturation.

        es  = 0.6108 * exp(17.27 * T / (T + 237.3))
        VPD = es * (1 - RH/100)

    This is the LEAF-TEMPERATURE-EQUALS-AIR-TEMPERATURE form, which is what the
    dashboard has always plotted. A leaf transpiring in sun sits below air
    temperature and its true VPD is lower; correcting for that needs a leaf
    temperature nothing here measures.
    """
    saturation = 0.6108 * math.exp((17.27 * temperature) / (temperature + 237.3))

    return saturation * (1 - (humidity / 100))
