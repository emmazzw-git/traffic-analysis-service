def write(output: list):
    for x in output:
        print(f"{x[0]}-{add_trailing_zero(x[1])}-{add_trailing_zero(x[2])} {str(x[3])}")


def add_trailing_zero(digit: int) -> str:
    return f"0{digit}" if len(str(digit)) == 1 else str(digit)
