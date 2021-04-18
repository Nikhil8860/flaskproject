from flask import Blueprint
from flask_restful import Resource, Api

login_user = Blueprint('login_user', __name__)
login_api = Api(login_user)


class Login(Resource):
    def get(self):
        return {"Success": "HELLO"}


login_api.add_resource(Login, '/login_user')
