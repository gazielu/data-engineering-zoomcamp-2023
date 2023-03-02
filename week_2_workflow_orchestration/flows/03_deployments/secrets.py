from prefect import flow
from prefect.blocks.system import Secret


@flow
def add_secret_block():
    Secret(value="0123456789").save(name="db_password")


@flow
def retrieve_secret():
    db_pwd = Secret.load("db_password").get()
    print(db_pwd)


if __name__ == "__main__":
    add_secret_block()
    retrieve_secret()
