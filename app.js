'use strict';

const express = require('express');
const elasticsearch = require('elasticsearch');
const bodyParser = require('body-parser');
const cors = require("cors");
const server = express();
var fs = require('fs'); 
var parse = require('csv-parse');
require('array.prototype.flatmap').shim()
var formidable = require('formidable');
const vega = require('vega')
const lite = require('vega-lite')

server.set('view engine', 'ejs');
server.use(express.static(__dirname + '/views'));


const client = new elasticsearch.Client({
  hosts: ['http://localhost:9200']
});

client.ping(
 {
  requestTimeout: 30000,
 }, 
 function(error) {
  if (error) {
   console.error('Cannot connect to Elasticsearch.');
   myres = 'Cannot connect to Elasticsearch.';
  } else {
   console.log('Connected to Elasticsearch was successful!');
  }
 });

server.use(cors());
server.use(bodyParser.json());


server.get('/', (req, res) => {

    var error = false;
    var result;
    var error_message;

    client.search({
        index: 'bank',
        size: 10000,
        }, function(err, resp, status){
          if(err){
            error_message = err;
            error = true;
          }else{
            
            result = resp.hits.hits;

          }

          res.render('index', {error: error, error_message: error_message, result: result});
      });

    
});


server.get('/visualize/age', (req, res) => {

    var error = false;
    var result;
    var error_message;

    client.search({
        index: 'bank',
        body: {
            "aggs": {
              "2": {
                "terms": {
                  "field": "age",
                  "order": {
                    "_count": "desc"
                  },
                  "size": 5
                }
              }
            },
            "size": 0,
            "stored_fields": [
              "*"
            ],
            "script_fields": {},
            "docvalue_fields": [],
            "_source": {
              "excludes": []
            },
            "query": {
              "bool": {
                "must": [],
                "filter": [
                  {
                    "match_all": {}
                  }
                ],
                "should": [],
                "must_not": []
              }
            }
          }
        }, function(err, resp, status){
          if(err){
            error_message = err;
            error = true;
            res.render('age', {error: error, error_message: error_message});
          }else{
            
            result = resp.hits.hits;

            var values = [];

          resp.aggregations["2"].buckets.forEach(function(elt){

            values.push({ages: elt.key, owners_count: elt.doc_count});
          });

          var yourVlSpec = {
            "width": 500,
            "height": 500,
            data: {
              values: values
            },
            mark: 'bar',
            encoding: {
              x: {field: 'ages', type: 'ordinal'},
              y: {field: 'owners_count', type: 'quantitative'}
            }
          };
          let vegaspec = lite.compile(yourVlSpec).spec
          var view = new vega.View(vega.parse(vegaspec), 
          {renderer: "none"})
    
          view.toSVG()
      .then(function(svg) {

        res.render('age', {error: error, error_message: error_message, result: result, image: svg});

      })
      .catch(function(err) { 
        res.render('age', {error: true, error_message: err});
      });

          }
        }
    );

});

server.get('/visualize/gender', (req, res) => {

  var error = false;
  var result;
  var error_message;

  client.search({
      index: 'bank',
      body: {
        "aggs": {
          "2": {
            "terms": {
              "field": "gender.keyword",
              "order": {
                "_count": "desc"
              },
              "size": 5
            }
          }
        },
        "size": 0,
        "stored_fields": [
          "*"
        ],
        "script_fields": {},
        "docvalue_fields": [],
        "_source": {
          "excludes": []
        },
        "query": {
          "bool": {
            "must": [],
            "filter": [
              {
                "match_all": {}
              }
            ],
            "should": [],
            "must_not": []
          }
        }
      }
      }, function(err, resp, status){
        if(err){
          error_message = err;
          error = true;
          res.render('gender', {error: error, error_message: error_message});
        }else{
          
          result = resp.hits.hits;

          var values = [];

        resp.aggregations["2"].buckets.forEach(function(elt){

          values.push({category: elt.key, value: elt.doc_count});

        });

        var yourVlSpec = {
          "width": 500,
          "height": 500,
          "data": {
            "values": values
          },
          "mark": "arc",
          "encoding": {
            "theta": {"field": "value", "type": "quantitative"},
            "color": {"field": "category", "type": "nominal"}
          },
          "view": {"stroke": null}
        };
        let vegaspec = lite.compile(yourVlSpec).spec
        var view = new vega.View(vega.parse(vegaspec), 
        {renderer: "none"})
  
        view.toSVG()
    .then(function(svg) {

      res.render('gender', {error: error, error_message: error_message, result: result, image: svg});

    })
    .catch(function(err) { 
      res.render('gender', {error: true, error_message: err});
    });

        }
      }
  );

});

server.get('/search', (req, res) => {

    var search = req.query.search;

    if(search == undefined || search == null){
        res.render('index', {error: true, error_message: "Your search was empty" , result: null});
    }
    else{

        var error = false;
        var result;
        var error_message;

        client.search({
            index: 'bank',
            body: {
                "query": {
                    "bool": {
                        "should":
                        [
                            {
                                "wildcard" : { "lastname" : search + "*" }
                            },
                            {
                                "wildcard" : { "firstname" : search + "*" }
                            },
                        
                        ]
                    }
                }
            }
        }, function(err, resp, status){
              if(err){
                error_message = err;
                error = true;
              }else{

                  result = resp.hits.hits;
              }

              res.render('index', {error: error, error_message: error_message, result: result});
          });
    }
    
});

server.get('/to_delete', (req, res) => {

    var id = req.query.id;

    if(id == undefined || id == null){
        res.render('index', {error: true, error_message: "You must select something to delete" , result: null});
    }
    else{

        var error = false;
        var result;
        var error_message;
        var to_delete = false;;
        

        client.get({
            index: 'bank',
            id: id
        }, function(err, resp, status){
              if(err){
                error_message = err;
                error = true;
              }else{

                result = resp._source;
                result._id = resp._id;
                to_delete = true;

              }

              res.render('delete', {to_delete: to_delete, error: error, error_message: error_message, result: result});
          });

    }
    
});

server.get('/to_add', (req, res) => {

    res.render('add', {error: false, error_message: null, result: null});
    
});

server.get('/to_edit', (req, res) => {

    var id = req.query.id;

    if(id == undefined || id == null){
        res.render('index', {error: true, error_message: "You must select something to edit" , result: null});
    }
    else{

        var error = false;
        var result;
        var error_message;
        var to_edit = false;;
        

        client.get({
            index: 'bank',
            id: id
        }, function(err, resp, status){
              if(err){
                error_message = err;
                error = true;
              }else{

                result = resp._source;
                result._id = resp._id;
                to_edit = true;

              }

              res.render('edit', {to_edit: to_edit, error: error, error_message: error_message, result: result});
          });

    }
    
});

server.get('/delete', (req, res) => {

    var id = req.query.id;

    if(id == undefined || id == null){
        res.render('index', {error: true, error_message: "You must select something to delete" , result: null});
    }
    else{

        var error = false;
        var result;
        var error_message;
        var was_deleted = false;
        

        client.delete({
            index: 'bank',
            id: id
        }, function(err, resp, status){
              if(err){
                error_message = err;
                error = true;
              }else{

                result = resp._source;
                was_deleted = true;

              }

              res.render('delete', {was_deleted: was_deleted, error: error, error_message: error_message, result: result});
          });

    } 
    
});

server.get('/add', (req, res) => {

    if(req.query.firstname == undefined || req.query.firstname == null){
        res.render('index', {error: true, error_message: "Missing fields while adding bank account" , result: null});
    }
    else{

        var error = false;
        var result;
        var error_message;
        var was_added = false;
    
        client.index({
            index: 'bank',
            body: {
                account_number: req.query.accountnumber,
                firstname: req.query.firstname,
                lastname: req.query.lastname,
                gender: req.query.gender,
                age: req.query.age,
                state: req.query.state,
                city: req.query.city,
                address: req.query.address,
                email: req.query.email,
                employer: req.query.employer,
                balance: req.query.balance
            }
        }, function(err, resp, status){
              if(err){
                error_message = err;
                error = true;
              }else{

                result = resp._source;
                was_added = true;

              }

              res.render('add', {was_added: was_added, error: error, error_message: error_message, result: result});
          });

    } 
    
});

server.post('/add_from_csv', (req, res) => {


    var form = new formidable.IncomingForm();
    form.parse(req, function (err, fields, files) {
      
        if(err){
          res.render('index', {error: true, error_message: err});
        }
        else{

            fs.createReadStream(files.filetoupload.path).pipe(
                parse({columns: true, delimiter: ";"}, function (err, records) {
        
                    const body = records.flatMap(doc => [{ index: { _index: 'bank' } }, doc]);
        
                    client.bulk({ refresh: true, body }, function(err, resp, status){
                          if(err){
                            res.render('index', {error: true, error_message: err});

                          }else{
          
                            res.render('index', {error_from_csv: false});
            
                          }
                        
                      });
                })
            );
        }
    });
    
});

server.get('/edit', (req, res) => {

    var id = req.query.id;

    if(id == undefined || id == null){
        res.render('index', {error: true, error_message: "You must select something to edit" , result: null});
    }
    else{

        var error = false;
        var result;
        var error_message;
        var was_edited = false;
        

        client.update({
            index: 'bank',
            id: id,
            body: {
                doc: {
                    firstname: req.query.firstname,
                    lastname: req.query.lastname,
                    gender: req.query.gender,
                    age: req.query.age,
                    state: req.query.state,
                    city: req.query.city,
                    address: req.query.address,
                    email: req.query.email,
                    employer: req.query.employer,
                    balance: req.query.balance
                }
            }
        }, function(err, resp, status){
              if(err){
                error_message = err;
                error = true;
              }else{

                result = resp._source;
                was_edited = true;

              }

              res.render('edit', {was_edited: was_edited, error: error, error_message: error_message, result: result});
          });

    } 
    
});

server.listen(8082);

console.log('Server running at http://127.0.0.1:8082/');