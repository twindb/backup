from tests.unittests.excluded_env_config.dummy_content_generation.dino_namegen import dino_gen
from tests.unittests.excluded_env_config.dummy_content_generation.cave_generation import cave_gen
from tests.unittests.excluded_env_config.dummy_content_generation.cave_painting_desc import painting_gen




def manual_dummy_file_creation(out_target:str, fsize:int):
    def rando_gen(total_len:int)->bytes:
        parts = total_len//3
        for grp in zip(dino_gen(parts), cave_gen(parts), painting_gen(parts)):
            yield from grp
    from pathlib import Path
    out_target = Path(out_target).resolve()
    out_target.parent.mkdir(parents=True,exist_ok=True)
    with open(out_target,"wb") as f:
        f.writelines(rando_gen(fsize))


if __name__ == '__main__':
    import concurrent.futures as cf
    import os
    from random import random
    oneg = 2**30
    mean = 2**33
    half_span = 2**32

    with cf.ProcessPoolExecutor(os.cpu_count()) as ppe:
        args = []
        for i,mult in enumerate([4]*5+[9]*5):
            size = int(oneg*mult+(oneg*random())*round(random()*2.-1.))
            name = f"./big_dummies/{size//oneg}_{i}.txt"
            args.append((name,size))
        args.sort(key=lambda s: s[1])
        ftrs = []
        for name,size in args:
            print(name,size)
            ftrs.append(ppe.submit(manual_dummy_file_creation,name,size))
        cf.wait(ftrs)
