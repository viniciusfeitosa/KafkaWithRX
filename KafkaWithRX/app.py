from consumer import MyImplementation
from initializer import RXInitializer
from flask import Flask

app = Flask(__name__)


@app.route("/")
def hello():
    return "Hello World"


if __name__ == "__main__":
    initializer = RXInitializer([
        MyImplementation(
            iterable=[
                {'key': 'name', 'value': 'Vinicius'},
                {'key': 'name', 'value': 'Feitosa'},
                {'key': 'name', 'value': 'Pacheco'},
            ]
        )
    ])
    initializer.run()
    app.run()
