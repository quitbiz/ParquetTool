import os
from flask import Flask, request, redirect, url_for, render_template
from flask_dropzone import Dropzone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


app = Flask(__name__)
dropzone = Dropzone(app)

app.config['DROPZONE_ALLOWED_FILE_CUSTOM'] = True
app.config['DROPZONE_ALLOWED_FILE_TYPE'] = '.parquet'
app.config['DROPZONE_MAX_FILE_SIZE'] = 3
app.config['UPLOAD_PATH'] = 'uploads'

if not os.path.exists(app.config['UPLOAD_PATH']):
    os.makedirs(app.config['UPLOAD_PATH'])

@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        f = request.files.get('file')
        if f:
            filename = f.filename
            filepath = os.path.join(app.config['UPLOAD_PATH'], filename)
            f.save(filepath)
            return redirect(url_for('show_table', filename=filename))
    return render_template('index.html')


@app.route("/table/<string:filename>", methods=['GET', 'POST'])
def show_table(filename):
    filepath = os.path.join(app.config['UPLOAD_PATH'], filename)
    table_html = ''
    if request.method == 'POST':
        df = pd.read_parquet(filepath)
        for col in df.columns:
            new_value = request.values.get(col)
            if new_value:
                df[col] = new_value
        pq.write_table(pa.Table.from_pandas(df), filepath)
        return redirect(url_for('show_table', filename=filename))
    else:
        try:
            table_html = pd.read_parquet(filepath).to_html()
        except:
            table_html = '<p>Error loading table from file.</p>'
    return render_template('show_table.html', table_html=table_html, filename=filename)


if __name__ == "__main__":
    app.run(debug=True)
