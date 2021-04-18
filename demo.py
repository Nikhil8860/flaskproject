from flask import Flask, current_app

app = Flask(__name__)
with app.app_context():
    # within this block, current_app points to app.
    print(current_app.name)
    current_app.extensions['name'] = "Nikhil"
    print(current_app.extensions)
    print(current_app.config.get('INDEX_TEMPLATE'))


if __name__ == '__main__':
    app.run(debug=True)
