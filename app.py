import os
from flask import Flask, request, render_template, redirect, url_for
from core import Model

# Constant Model Configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg'])

# Flask Initialization and Configuration
app = Flask(__name__)
model = Model()

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/chat', methods=['GET'])
def submit():
    message = request.args.get('message', '') 
    response = model.chat(message)
    return response

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
