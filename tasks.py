from invoke import task


@task
def hello(c):
    print("Hello, World!")


@task
def run(c):
    c.run("streamlit run app/app.py")


@task
def requirements(c):
    c.run(
        "uv export --frozen --no-dev --no-hashes --no-annotate -o app/requirements.txt"
    )
