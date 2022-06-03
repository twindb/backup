from subprocess import PIPE, Popen


def test_create_bucket(gs):
    proc = Popen(["bash", "-c", "echo 123"], stdout=PIPE, stderr=PIPE)
    gs.save(proc.stdout, "test")
    proc.wait()
    # print(cout)
