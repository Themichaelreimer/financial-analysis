import sys
import flask
from flask import request, abort, g
from functools import wraps
from Database import *

app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.secret_key = "bccd5bcdff0bf58b26a5"
API_KEY = "KuLA15DiVV6Z6b5KzG1c3OEPbtNGRPbhQan8E9aL"


@app.before_first_request
def get_db():

    db = getattr(g, '_database', None)

    if db is None:
        db = g._database = Database()
    return db

@app.teardown_request
def close_connection(self):
    db = get_db()
    db.conn.close()
    print("Connection Closed")
    
    
def require_key(view_function):
    @wraps(view_function)
    def decorated_function(*args, **kwargs):
        if dev_mode():
            return view_function(*args, **kwargs)
        keys = get_keys()
        if request.headers.get('key') and request.headers.get('key') in keys:
            return view_function(*args, **kwargs)
        else:
            abort(401)
    return decorated_function
    
    
def dev_mode():
    return "--dev" in sys.argv
    
    
def fail(msg):
    flask.abort(400,msg)
        
        
@app.route('/', methods=['GET'])
def home():
    return "Welcome!"
        
        
@app.route('/active_symbols/', methods=['GET'])
@require_key
def get_active_symbols():
    db = get_db()
    symbols = db.get_relevent_symbols()
    return flask.jsonify(symbols)
    
    
@app.route('/updatedtime/<symbol>/', methods=['GET'])
@require_key
def get_updated_time(symbol):
    db = get_db()
    data = db.get_symbol_updated_time(symbol)
    return flask.jsonify(data)
    
    
@app.route('/get/<symbol>/', methods=['GET'])
@require_key
def get_symbol(symbol):
    db = get_db()
    data = db.get_symbol_data(symbol)
    return flask.jsonify(data)

if __name__ == '__main__':
    app.run()
