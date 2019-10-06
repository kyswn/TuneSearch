#!/usr/bin/python3

from flask import Flask, render_template, request

import search

application = app = Flask(__name__)
app.debug = True
offset_from = 0
num_results = 0

@app.route('/search', methods=["GET"])
def dosearch():
    global offset_from
    global num_results
    option = 'hold'
    query = request.args['query']
    qtype = request.args['query_type']
    qbutton = request.args['button']
    if (qbutton == 'Next' and offset_from <= (num_results - 20)):
        offset_from += 20
    if (qbutton == 'Previous' and offset_from >= 20):
        offset_from -= 20
    if (qbutton == 'Search'):
        offset_from = 0
        option = 'recompute'
    """
    TODO:
    Use request.args to extract other information
    you may need for pagination.
    """

    search_results = search.search(query, qtype, offset_from, option)
    if (qbutton == 'Search'):
        num_results = search_results[1]
    results_showing = num_results - search_results[2]
    return render_template('results.html',
            query=query,
            results=num_results,
            search_results=search_results[0],
            remaining=results_showing,
            current=search_results[2])

@app.route("/", methods=["GET"])
def index():
    if request.method == "GET":
        pass
    return render_template('index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0')
