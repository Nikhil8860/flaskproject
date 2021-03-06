"""
Global Variable as per Flipkart chart of commission
"""
import pymysql.cursors
from datetime import date
import json

WEIGHT_LIST = None


def get_rate_card(order_date):
    try:
        connection = pymysql.connect('172.31.0.81', 'root', 'evanik@2019', f'evanik_main')
        cursor = connection.cursor()
        date_now = date(2021, 1, 1)
        if order_date >= date_now:
            query = f"""select extra_2 from charges_variance_rate_card 
                        where fee_type='shipping fee' and valid_from='{date_now}'"""
            cursor.execute(query)
            data = cursor.fetchone()
        else:
            query = f"""select extra_2 from charges_variance_rate_card 
                        where fee_type='shipping fee' and valid_to<'{date_now}'"""
            cursor.execute(query)
            data = cursor.fetchone()

        if data:
            data = data[0]

        weight_slab = list(json.loads(data)['WEIGHT_SLAB'].values())
        local = [int(i) for i in json.loads(data)['LOCAL'].values()]
        zonal = [int(i) for i in json.loads(data)['ZONAL'].values()]
        national = [int(i) for i in json.loads(data)['NATIONAL'].values()]

    except Exception as e:
        print("Error while connecting " + str(e))
    finally:
        cursor.close()
        connection.close()
    return weight_slab, local, zonal, national


def cal_payment(weight, zone, tiers, order_date) -> float:
    """
    This Function will take weight and zone
    and calculate the price as the the zone
    :param weight:
    :param zone:
    :return: price
    """

    WEIGHT_SLAB, LOCAL, ZONAL, NATIONAL = get_rate_card(order_date)
    print(weight, zone, tiers, sep='---')
    global WEIGHT_LIST
    slab, weight = check_slab(weight)
    multiply_factor = 1
    if slab and weight:
        try:
            index = WEIGHT_SLAB.index(slab)
            PRICE = 0.0
            #  Check for the Zone
            if zone.lower() == 'local':
                WEIGHT_LIST = LOCAL
            elif zone.lower() == 'zonal':
                WEIGHT_LIST = ZONAL
            elif zone.lower() == 'national':
                WEIGHT_LIST = NATIONAL
            else:
                pass

            for i, v in enumerate(WEIGHT_SLAB[: index + 1]):
                if weight > 0.0 and v != '2.0-3.0' and v != '3.0-12.0' and v != slab:
                    PRICE += float(WEIGHT_LIST[i]) * float(multiply_factor)
                    weight -= 0.5 * multiply_factor
                elif v == '2.0-3.0' and weight > 0.0 and v != slab and v != '3.0-12.0':
                    multiply_factor = 2
                    PRICE += float(WEIGHT_LIST[i]) * float(multiply_factor)
                    weight -= (0.5 * multiply_factor)
                elif v == '3.0-12.0':
                    if weight >= 9.0:
                        multiply_factor = 9.0
                        PRICE += float(WEIGHT_LIST[i]) * float(multiply_factor)
                        weight -= 9.0
                        i += 1
                    if weight > 0.0:
                        v = WEIGHT_SLAB[-1]
                        multiply_factor = weight
                        PRICE += float(WEIGHT_LIST[i]) * float(multiply_factor)
                else:
                    PRICE += float(WEIGHT_LIST[i]) * float(multiply_factor)

            #  check for the tiers

            if tiers.lower() == 'bronze':
                return PRICE
            elif tiers.lower() == 'gold':
                return PRICE * 0.8
            elif tiers.lower() == 'silver':
                return PRICE * 0.9
        except Exception as e:
            print("CALCULATOR" + str(e))


def check_slab(weight) -> tuple:
    """
    This function will take weight from the user and calculate the weight
    :param weight: user will give
    :return: object
    """
    if weight:
        if (float(weight) >= 0.0) and (float(weight) <= 0.5):
            slab = '0.0-0.5'
            return slab, weight

        elif (float(weight) >= 0.5) and (float(weight) <= 1.0):
            slab = '0.5-1.0'
            return slab, weight

        elif (float(weight) >= 1.0) and (float(weight) <= 1.5):
            slab = '1.0-1.5'
            return slab, weight

        elif (float(weight) >= 1.5) and (float(weight) <= 2.0):
            slab = '1.5-2.0'
            return slab, weight

        elif (float(weight) >= 2.0) and (float(weight) <= 3.0):
            slab = '2.0-3.0'
            return slab, weight

        elif (float(weight) >= 3.0) and (float(weight) <= 12.0):
            slab = '3.0-12.0'
            return slab, weight

        else:
            slab = '>12.0'
            return slab, weight
    else:
        print("Weight None")
        return None, None
