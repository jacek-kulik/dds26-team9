import requests

BASE = "http://localhost:8000"

def verify(user_id="0", item_id="0", initial_credit=1000, initial_stock=100, price=10):

    stock = requests.get(f"{BASE}/stock/find/{item_id}").json()["stock"]
    credit = requests.get(f"{BASE}/payment/find_user/{user_id}").json()["credit"]

    stock_sold = initial_stock - stock
    money_spent = initial_credit - credit

    print("Stock sold:", stock_sold)
    print("Money deducted:", money_spent)

    expected_money = stock_sold * price

    if money_spent != expected_money:
        print("FAIL: atomicity broken")
        return False

    if stock < 0:
        print("FAIL: negative stock")
        return False

    if credit < 0:
        print("FAIL: negative credit")
        return False

    print("PASS: system consistent")
    return True