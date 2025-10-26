from invoke.tasks import task


@task
def run(c):
    c.run("streamlit run app/Home.py")


@task
def requirements(c):
    c.run(
        "uv export --frozen --no-dev --no-hashes --no-annotate -o app/requirements.txt"
    )
