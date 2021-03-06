"""
Global Variable as per Flipkart chart of commission
"""
import math

STEP_LEVEL = ['Standard', 'All levels']
WEIGHT_SLAB = ['0.0-0.5', '0.5-1.0', '1.0-5.0', '>5.0']
LOCAL = [38, 16, 13, 10]
REGIONAL = [48, 21, 18, 11]
NATIONAL = [69, 26, 24, 17]

WEIGHT_LIST = None


def cal_payment(weight, zone, tiers) -> float:
    """
    This Function will take weight and zone
    and calculate the price as the the zone
    :param weight:
    :param zone:
    :return: price
    """
    global WEIGHT_LIST
    slab, weight = check_slab(weight)
    multiply_factor = 1
    index = WEIGHT_SLAB.index(slab)
    PRICE = 0.0
    #  Check for the Zone
    if zone.lower() == 'local':
        WEIGHT_LIST = LOCAL
    elif zone.lower() == 'regional':
        WEIGHT_LIST = REGIONAL
    elif zone.lower() == 'national':
        WEIGHT_LIST = NATIONAL
    else:
        pass
    for i, v in enumerate(WEIGHT_SLAB[: index + 1]):
        if weight > 0.0 and v != '1.0-5.0' and v != '>5.0':
            PRICE += float(WEIGHT_LIST[i]) * float(multiply_factor)
            weight -= 0.5
        elif weight > 0.0 and v == '1.0-5.0' and v != '>5.0':
            multiply_factor = 4.0
            PRICE += float(WEIGHT_LIST[i]) * float(multiply_factor)
            weight -= 4
        elif weight > 0:
            PRICE += float(WEIGHT_LIST[i]) * float(math.ceil(weight))
            weight -= 5

    if tiers.lower() == 'bronze':
        return PRICE
    elif tiers.lower() == 'gold':
        return PRICE * 0.8
    else:
        return PRICE * 0.9


def check_slab(weight) -> tuple:
    """
    This function will take weight from the user and calculate the weight
    :param weight: user will give
    :return: object
    """

    if (float(weight) >= 0.0) and (float(weight) < 0.5):
        slab = '0.0-0.5'
        return slab, weight

    elif (float(weight) >= 1.0) and (float(weight) < 5.0):
        slab = '1.0-5.0'
        return slab, weight

    else:
        slab = '>5.0'
        return slab, weight
