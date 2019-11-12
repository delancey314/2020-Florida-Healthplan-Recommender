from flask import Flask, request, render_template

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('landing.html')


@app.route('/conditions', methods=['GET', 'POST'])
def conditions():
    return render_template('conditions_experiment.html')


@app.route('/analysis', methods=['GET', 'POST'])
def analysis():
        if request.method == 'POST':
            listx = request.form.getlist('mycheckbox')
            results=new_pipeline(listx)

        return 'Done'
    return render_template('results.html')

'''
@app.route('/demography',methods=['GET', 'POST'])
def demography():
    return render_template('landing.html')
'''

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8105, threaded=True)
