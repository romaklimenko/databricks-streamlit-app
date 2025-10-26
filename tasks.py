from invoke import task


@task
def hello(c):
    print("Hello, World!")


@task
def run(c):
    c.run("streamlit run app/app.py")
