class MenuItem:
    def __init__(self, name, water, milk, coffee, cost):
        self.name = name
        self.water = water
        self.milk = milk
        self.coffee = coffee
        self.cost = cost
        
        
class CoffeeMachine: #drink size
    def __init__(self):
        self._water = 300
        self._milk = 100
        self._coffee = 500
        self._money = 0.0
        self.menu = {
            "espresso": MenuItem("espresso", water=50, milk=0, coffee=18, cost=1.5),
            "latte": MenuItem("latte", water=200, milk=150, coffee=24, cost=2.5),
            "cappuccino": MenuItem("cappuccino", water=250, milk=100, coffee=24, cost=3)
        }
        
    def report(self):
        return (f"Water: {self._water}ml, Milk: {self._milk}ml, Coffee: {self._coffee}, Cost: ${self._money:.2f}")
    
    def _is_resource_sufficient(self, item):
        if self._water < item.water:
            return False, "water"
        if self._milk < item.milk:
            return False, "milk"
        if self._coffee < item.coffee:
            return False, "coffee"
        return True, ""
    
    def _process_coin(self, inserted_coin):
        total = sum(inserted_coin)
        return total
    
    def make_coffee(self, drink_name, inserted_coin):
        if drink_name not in self.menu:
            return False, "Invaild drink choice"
        
        item = self.menu[drink_name]
        sufficient, lacking = self._is_resource_sufficient(item)
        if not sufficient:
            return False, f"Sorry, not enough {lacking}"
        
        total_inserted = self._process_coin(inserted_coin)
        if total_inserted < item.cost:
            return False, "Sorry, that's not enough money"
        
        self._water -= item.water
        self._milk -= item.milk
        self._coffee -= item.coffee
        self._money += item.cost
        
        change = round(total_inserted - item.cost, 2)
        return True, f"Enjoy is your {item.name}, your change is: {change}"