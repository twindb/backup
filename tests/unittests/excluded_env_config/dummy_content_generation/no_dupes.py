


def kill_dupes(fp:str):
    s = set()
    with open(fp,"rb") as fin:
        fin.seek(0)
        with open("no_dupes.txt","wb") as fout:
            for line in fin.readlines():
                line = line.strip()
                if line not in s:
                    s.add(line)
                    fout.write(line+b"\n")

if __name__ == '__main__':
    kill_dupes("big_file.txt")
