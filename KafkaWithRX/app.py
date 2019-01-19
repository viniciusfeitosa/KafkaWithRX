from consumer import MyImplementation
from initializer import rx_initializer
from flask import Flask

app = Flask(__name__)


@app.route("/")
def hello():
    return "Hello World"


if __name__ == "__main__":
    rx_initializer(
        MyImplementation(
            iterable=[
                {'key': 'name', 'value': 'Vinicius'},
                {'key': 'name', 'value': 'Feitosa'},
                {'key': 'name', 'value': 'Pacheco'},
            ]
        )
    )
    app.run()
