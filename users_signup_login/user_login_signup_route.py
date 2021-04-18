from flask import Flask
from signup import signup_user
from login import login_user


server = Flask(__name__)

with server.app_context():
    server.register_blueprint(signup_user)
    server.register_blueprint(login_user)
