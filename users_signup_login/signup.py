from flask import Blueprint, request, jsonify, make_response
from flask_restful import Resource, Api
from datetime import datetime
from sqlalchemy import create_engine
import logging

# logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(message)s', )

signup_user = Blueprint('signup_user', __name__)
sign_api = Api(signup_user)

engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/users')


class SignUp(Resource):
    def post(self):
        try:
            name = request.form['name']
            email = request.form['email']
            date_time = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")

            return make_response(jsonify({"name": name, "email": email, "date_time": date_time}), 201)
        except Exception as e:
            logging.error('Error', exc_info=True)
            return make_response(jsonify({"msg": str(e)}), 401)


sign_api.add_resource(SignUp, '/api/new/user/register')
