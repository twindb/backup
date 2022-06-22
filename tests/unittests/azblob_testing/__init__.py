from pathlib import Path

HERE = Path(__file__).parent


def do_set_osenvs(setter_func):
    here = Path(HERE)
    target_dummies = None
    while target_dummies is None and here.name:
        if "dummy_env_vars.json" not in here.iterdir():
            here = here.parent
        else:
            target_dummies = str(here.joinpath("dummy_env_vars.json"))
    setter_func(target_dummies)


PART_NAMES = "store,host,container,interval,media_type,fname".split(",")
SAMPLE_TARGETS = [
    "../../../.../.../mysql-2020-07-29_01_00_03.xbstream.gz",
    "../../mysql/some/extra/dirs/mysql-2020-07-29_01_00_03.xbstream.gz",
    "../../../../../mysql-fullbackup-qa1-rms",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_03_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_04_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_05_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_06_00_04.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_07_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_08_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_09_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_10_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_11_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_12_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_13_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_14_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_15_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_16_00_04.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_17_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_18_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_19_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_20_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_21_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_22_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-28_23_00_03.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-29_00_05_13.xbstream.gz",
    "s3://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-29_01_00_03.xbstream.gz",
    "azure://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-29_01_00_03.xbstream.gz",
    "azure://{host}/mysql-fullbackup-qa1-rms/hourly/mysql/mysql-2020-07-29_01_00_03.xbstream.gz",
]
