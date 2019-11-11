from flask import Flask, render_template
app=Flask(__name__)

@app.route('/')
def index():
    return render_template('landing.html')

@app.route('/demography')
def index():
    return render_template('landing.html')


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8105, threaded=True)
