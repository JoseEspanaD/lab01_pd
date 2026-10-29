from importlib import import_module


def test_inventory_package_imports():
    package = import_module("inventory")
    assert package.__name__ == "inventory"
