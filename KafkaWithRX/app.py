from consumer import MyImplementation1
from initializer import RXInitializer
from flask import Flask

app = Flask(__name__)


@app.route("/")
def hello():
    return "Hello World"


if __name__ == "__main__":
    initializer = RXInitializer([
        MyImplementation1()
    ])
    initializer.run()
    app.run()
