from itertools import combinations
from random import randint

descriptors = "simple,busy,abstract,obvious,pretty,scary,large,small,relatable,detailed,complex".split(",")
combos = tuple(tuple(combinations(descriptors,i)) for i in range(1,5))
combo_len = len(combos)-1
lens = tuple(len(c)-1 for c in combos)


def painting_gen(result_len:int):
    byte_count = 0
    while byte_count < result_len:
        combo_idx = randint(0,combo_len)
        clen = lens[combo_idx]
        combo = combos[combo_idx][randint(0,clen)]
        if len(combo)>2:
            combo = ", ".join(combo[:-1])+f", and {combo[-1]}"
        elif len(combo)==2:
            combo = ", ".join(combo[:-1]) + f" and {combo[-1]}"
        else:
            combo = combo[0]
        val = f"{combo} types of cave-paintings\n".capitalize().encode("utf-8")
        yield val
        byte_count += len(val)


if __name__ == '__main__':
    for s in painting_gen(10):
        print(s)
