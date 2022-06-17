"""Filler content generator.
code inspired by that used to create the site: https://www.fantasynamegenerators.com/dinosaur-names.php"""
from random import sample
from itertools import combinations
import multiprocessing as mp
from multiprocessing import Queue
from queue import Empty
from time import perf_counter

colors = (
    ("fuscus", "dark"),
    ("Nigri", "Black"),
    ("aterum", "dark-black"),
    ("lividus", "blue-black"),
    ("Cyano", "Blue"),
    ("Leuco", "White"),
    ("Chloro", "Green"),
    ("prasino", "green"),
    ("purpureus", "purple"),
    ("caeruleus", "cerulean"),
    ("ravus", "gray"),
    ("canus", "light-gray"),
    ("albus", "white"),
    ("Xantho", "Yellow"),
    ("flavus", "yellow"),
    ("fulvus", "golden"),
    ("aurantium", "orange"),
    ("croceus", "saffron"),
    ("ruber", "red"),
    ("roseus", "rose-red"),
)
colors = tuple(
    pair for tpl in zip(colors,(("","") for _ in range(len(colors)))) for pair in tpl
)
physical_descriptors1 = (
    ("rhyncho", "Beak"),("chelo", "Claw"),("podo", "Foot"),("cerco", "Tail"),("canto", "Spined"),("cephalo", "Headed"),
    ("donto", "Teeth"),("don", "Tooth"),("lopho", "Crested"),("ploce", "Armored"),("plo", "Armored"),("rhino", "Nosed"),
    ("trachelo", "Necked"),("minisculum", "extremely-small"),("exigum","very-small"),("minimum", "tiny"),("parvum", "small"),
    ("vegrande", "not-very-big"),("praegrande", "very-big"),("magnum", "great"),("enorme", "enormous"),("immane", "huge"),
    ("immensum", "immense"),("vastum", "vast"),("",""),
)
physical_descriptors2 = (
    ("Acantho", "Spiny"),("Acro", "High"),("Aegypto", "Egyptian"),("Aepy", "Tall"),("Afro", "African"),
    ("Agili", "Agile"),("Alectro", "Eagle"),("Ammo", "Sand"),("Anchi", "Near"),("Ankylo", "Stiff"),
    ("Antarcto", "Antarctic"),("Apato", "Deceptive"),("Archaeo", "Ancient"),("Arrhino", "Without Nose"),
    ("Austro", "South"),("Avi", "Bird"),("Baga", "Small"),("Baro", "Heavy"),("Bellu", "Fine"),("Brachio", "Arm"),
    ("Brachy", "Short"),("Callio", "Beautiful"),("Campto", "Bent"),("Carno", "Carnivorous"),("Cerato", "Horned"),
    ("Chloro", "Green"),("Coelo", "Hollow"),("Colosso", "Giant"),("Cyano", "Blue"),("Cyclo", "Round"),
    ("Cyrto", "Curved"),("Daspleto", "Frightful"),("Deino", "Terrible"),("Di", "Two"),("Dicraeo", "Forked"),
    ("Dilipho", "Two Ridged"),("Draco", "Dragon"),("Dromaeo", "Running"),("Drypto", "Tearing"),("Echino", "Spiny"),
    ("Elaphro", "Fleet"),("Eo", "Dawn"),("Eu", "Well"),("Gampso", "Curved"),("Gorgo", "Fierce"),("Gymno", "Bare"),
    ("Gyro", "Round"),("Hadro", "Big"),("Haplo", "Simple"),("Hespero", "Western"),("Hetero", "Different"),
    ("Hylaeo", "Woodland"),("Kentro", "Spiky"),("Krito", "Noble"),("Lasio", "Hairy"),("Lepto", "Slim"),
    ("Leuco", "White"),("Lopho", "Crested"),("Lurdu", "Heavy"),("Macro", "Large"),("Masso", "Massive"),
    ("Mega", "Large"),("Megalo", "Big"),("Metria", "Moderately"),("Micro", "Tiny"),("Mono", "Single"),
    ("Nano", "Dwarf"),("Nano", "Tiny"),("Neo", "New"),("Nigri", "Black"),("Oro", "Mountain"),
    ("Orycto", "Digging"),("Ovi", "Egg"),("Pachy", "Thick"),("Parali", "Tidal"),("Peloro", "Monstrous"),
    ("Plateo", "Flat"),("Platy", "Flat"),("Pogono", "Bearded"),("Preno", "Sloping"),("Prenoce", "Sloping"),
    ("Pro", "Before"),("Proto", "Before"),("Rhab", "Rod"),("Rugos", "Wrinkled"),("Salto", "Hopping"),
    ("Sarco", "Flesh"),("Segno", "Slow"),("Silvi", "Forest"),("Sino", "Chinese"),("Spino", "Thorn"),
    ("Stego", "Roof"),("Steno", "Narrow"),("Styraco", "Spiked"),("Super", "Super"),("Theco", "Socket"),
    ("Therizino", "Scythe"),("Thescelo", "Wonderful"),("Toro", "Bull"),("Torvo", "Savage"),("Trachy", "Rough"),
    ("Trichodo", "Hairy"),("Troo", "Wounding"),("Tyloce", "Swelling"),("Tyranno", "Tyrant"),("Veloci", "Quick"),
    ("Xantho", "Yellow"),("","")
)
abstract_descriptors1 = (
    ("bator", "Hero"), ("ceratops", "Horned Face"), ("draco", "Dragon"), ("dromeus", "Runner"), ("gryphus", "Griffin"),
    ("lestes", "Stealer"), ("mimus", "Mimic"), ("moloch", "Demon"), ("raptor", "Plunderer"), ("rex", "King"),
    ("sauropteryx", "Winged Lizard"), ("saurus", "Lizard"), ("saura", "Lizard"), ("sornis", "Bird"), ("titan", "Giant"),
    ("tyrannus", "Tyrant"), ("venator", "Hunter"),("amorabundum", "loving"),("excitum", "excited"),("confūsum", "confused"),
    ("detestabile", "hateful"),("felix", "happy"),("invidum", "envious"),("iratum", "irate"),("laetum", "joyful"),
    ("miserum", "miserable"),("solum", "lonely"),("somnolentum", "sleepy"),("territum", "terrified"),("triste", "sad"),
    ("bella","beautiful"),("breve","short"),("cānum","gray-haired"),("casuale","casual"),("decens","proper"),
    ("decorum","well-mannered"),("deforme","ugly"),("elegans","elegant"),("flāvum","blonde"),("formale","formal"),
    ("iuvene","young"),("longe","tall"),("rūfum","red-haired"),("venustum","lovely"),("venustum","charming"),("vetere","old"),("",""),
)
abstract_descriptors2 = (
    ("don", "Tooth"),("bator", "Hero"),("canthus", "Spine"),("ceras", "Roof"),("ceratops", "Horned Face"),
    ("docus", "Neck"),("draco", "Dragon"),("dromeus", "Runner"),("gryphus", "Griffin"),("lestes", "Stealer"),
    ("lodon", "Tooth"),("mimus", "Mimic"),("moloch", "Demon"),("nychus", "Claw"),("pelix", "Pelvis"),
    ("pelta", "Shield"),("cephalus", "Head"),("pteryx", "Wing"),("pus", "Foot"),("raptor", "Plunderer"),
    ("rex", "King"),("rhinus", "Snout"),("rhothon", "Nose"),("sauropteryx", "Winged Lizard"),
    ("saurus", "Lizard"),("saura", "Lizard"),("sornis", "Bird"),("spondylus", "Vertebrae"),
    ("suchus", "Crocodile"),("tholus", "Dome"),("titan", "Giant"),("tyrannus", "Tyrant"),("venator", "Hunter"),("","")
)

colors = [(v1.strip(),v2.strip()) for v1,v2 in colors]
physical_descriptors1 = [(v1.strip(),v2.strip()) for v1,v2 in physical_descriptors1]
physical_descriptors2 = [(v1.strip(),v2.strip()) for v1,v2 in physical_descriptors2]
abstract_descriptors1 = [(v1.strip(),v2.strip()) for v1,v2 in abstract_descriptors1]
abstract_descriptors2 = [(v1.strip(),v2.strip()) for v1,v2 in abstract_descriptors2]


def combination_gen(_colors):
    def inner():
        phys = physical_descriptors1 + physical_descriptors2
        abst = abstract_descriptors1 + abstract_descriptors2
        combos = combinations((_colors,physical_descriptors1, physical_descriptors2, abstract_descriptors1, abstract_descriptors2), 5)
        for la, a in _colors:
            for lp1, p1 in phys:
                for lp2, p2 in abst:
                    if a + p1 + p2:
                        yield "".join(v for v in (la,lp1,lp2) if v) + " " +" ".join(v for v in (a, p1, p2) if v)
        for l1, l2, l3, l4, l5 in combos:
            for lp1, p1 in l1:
                for lp2, p2 in l2:
                    for lp3, p3 in l3:
                        for lp4, p4 in l4:
                            for lp5, p5 in l5:
                                if p1 + p2 + p3 + p4 + p5:
                                    yield "".join(v for v in (lp1,lp2,lp3,lp4,lp5) if v) + " " + " ".join(v for v in (p1, p2, p3, p4, p5) if v)
    for name in inner():
        if len(name.split()) > 1:
            yield name.capitalize().encode("utf-8")


def dino_gen(result_len:int, clrs=None):
    data = []
    byte_count = 0
    if clrs is None:
        clrs = colors
    for d in combination_gen(clrs):
        d += b'\n'
        yield d
        data.append(d)
        byte_count += len(d)
        if byte_count>=result_len:
            break
    while byte_count < result_len:
        for i in sample(range(len(data)),len(data)):
            d = data[i]
            yield d
            data.append(d)
            byte_count += len(d)
            if byte_count >= result_len:
                break


def _gen_wrapper(result_len:int, q:Queue, clrs:tuple=None):
    if not clrs:
        clrs = colors
    batch = []
    for n in dino_gen(result_len,clrs):
        batch.append(n)
        if len(batch)>1000:
            q.put(b''.join(batch))
            batch = []
    if batch:
        q.put(b''.join(batch))


def all_gen(chunk_bytes:int, num_chunks:int):
    clr_span = (len(colors)+num_chunks-1)//num_chunks
    q = Queue()
    procs = []
    for i in range(0, len(colors), clr_span):
        procs.append(mp.Process(target=_gen_wrapper, args=(chunk_bytes, q, colors[i:i + clr_span])))
    try:
        for proc in procs:
            proc.start()
        with open("big_file.txt","wb") as f:
            strt = perf_counter()
            batch = []
            elapsed = perf_counter()-strt
            while elapsed<5:
                print(f"\r[{'|'*(round(100*elapsed/5)):<100}]",end="")
                try:
                    v = q.get(True,.5)
                    if v:
                        batch.append(v)
                        strt = perf_counter()
                except Empty:
                    pass
                if len(batch)>10:
                    f.write(b''.join(batch))
                    if f.tell()>=(10*2**29):
                        break
                    batch = []
                elapsed = perf_counter() - strt
            if batch:
                f.writelines(batch)
    finally:
        for proc in procs:
            try:
                proc.join(2)
                if proc.exitcode is None:
                    try:
                        proc.terminate()
                    except:
                        pass
            except:
                try:
                    proc.terminate()
                except:
                    pass
            try:
                proc.close()
            except:
                pass


if __name__ == '__main__':
    all_gen(2**29, 10)
