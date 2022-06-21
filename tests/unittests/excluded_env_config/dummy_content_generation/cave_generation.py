from random import randint

biome = (
    ("swamp", "-", "y ", "ed "),
    ("desert", "-", "ed "),
    (
        "savanna",
        "-",
    ),
    ("mountain", "-", "ous ", "y-"),
    ("hill", "top ", "y "),
    ("valley", "-", "_floor "),
)
biome = tuple(v for tpl in zip(biome, (("", "") for _ in range(len(biome)))) for v in tpl)
feel = (
    "cozy ",
    "damp ",
    "dank ",
    "spacious ",
    "stinky ",
    "pleasant ",
    "small ",
    "large ",
    "big ",
    "dirty ",
    "clean ",
)
look = "open ,hidden ,exposed ,recessed ,majestic ,underwhelming ,high ,low ,deep ,shallow ".split(",")


def cave_gen(result_len: int):
    alen = len(biome) - 1
    blen = len(feel) - 1
    clen = len(look) - 1
    byte_count = 0
    while byte_count < result_len:
        a = biome[randint(0, alen)]
        a = a[0] + a[randint(1, len(a) - 1)]
        b = feel[randint(0, blen)]
        c = look[randint(0, clen)]
        abc = a + b + c if a.endswith("y ") or a.endswith("ed ") else b + c + a
        abc = abc.replace("-", " ").replace("_", "-")
        val = f"A {abc}cave\n".capitalize().encode("utf-8")
        yield val
        byte_count += len(val)


if __name__ == "__main__":
    for cave in cave_gen(100):
        print(cave)
