def print_tree(tree):
    print(type(tree).__name__)
    print_sub_tree(tree, indent=0)


def print_sub_tree(tree, indent=0):
    for c in tree.children:
        print(
            "|" + "-" * indent,
            type(c).__name__,
            "\033[0;34m" + (c.symbol.text if hasattr(c, "symbol") else "") + "\033[0m"
        )
        if hasattr(c, 'children') and c.children:
            print_sub_tree(c, indent + 2)
