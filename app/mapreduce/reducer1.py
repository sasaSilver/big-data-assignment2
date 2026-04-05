import sys


def parse_line(line):
    parts = line.strip().split("\t")
    if len(parts) != 2:
        return None, None
    key, value = parts
    
    if key.startswith("TT:"):
        return key, value
    
    if value.isdigit():
        return key, int(value)

    return None, None


def main():
    current_key = None
    current_sum = 0

    for line in sys.stdin:
        key, value = parse_line(line)
        if key is None:
            continue

        if key.startswith("TT:"):
            print(f"{key}\t{value}")
            continue

        if current_key == key:
            current_sum += value
        else:
            if current_key:
                print(f"{current_key}\t{current_sum}")
            current_key = key
            current_sum = value

    if current_key:
        print(f"{current_key}\t{current_sum}")


if __name__ == "__main__":
    main()
