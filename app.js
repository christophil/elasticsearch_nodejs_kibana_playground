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
            console.log(err);
            error_message = err;
            error = true;
          }else{
            
            result = resp.hits.hits;

          }

          var yourVlSpec = {
            $schema: 'https://vega.github.io/schema/vega-lite/v2.0.json',
            description: 'A simple bar chart with embedded data.',
            data: {
              values: [
                {a: 'A', b: 28},
                {a: 'B', b: 55},
                {a: 'C', b: 43},
                {a: 'D', b: 91},
                {a: 'E', b: 81},
                {a: 'F', b: 53},
                {a: 'G', b: 19},
                {a: 'H', b: 87},
                {a: 'I', b: 52}
              ]
            },
            mark: 'bar',
            encoding: {
              x: {field: 'a', type: 'ordinal'},
              y: {field: 'b', type: 'quantitative'}
            }
          };
          let vegaspec = lite.compile(yourVlSpec).spec
          var view = new vega.View(vega.parse(vegaspec), 
          {renderer: "none"})
    
          view.toSVG()
      .then(function(svg) {

        res.render('index', {error: error, error_message: error_message, result: result, image: svg});

        //res.send(svg);
      })
      .catch(function(err) { console.error(err); });

          
      });

    
});


//GET specific bank by id
server.get('/bank/:id', (req, res) => {
    var bank;
    client.get({
      index: 'bank',
      type: 'account',
      id: req.params.id
      }, function(err, resp, status){
        if(err){
          console.log(err);
        }else{
          bank = resp._source;
          console.log('found the requested document', resp);
          if (!bank){
            return res.status(400).send({
              message: 'bank is not found for id ${req.params.id}'
            });
          }
          return res.status(200).send({
            message: 'Get bank calls for id ${req.params.id} succeeded',
            bank: bank
          });
        }
    });
   });

//GET specific bank by id
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
                console.log(err);
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
                console.log(err);
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
                console.log(err);
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
                console.log(err);
                error_message = err;
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
        res.render('index', {error: true, error_message: "Missing filed while adding bank account" , result: null});
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
                console.log(err);
                error_message = err;
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
            console.log(err);
        }
        else{

            fs.createReadStream(files.filetoupload.path).pipe(
                parse({columns: true, delimiter: ";"}, function (err, records) {
        
                    console.log(records);
        
                    const body = records.flatMap(doc => [{ index: { _index: 'bank' } }, doc]);
        
                    client.bulk({ refresh: true, body }, function(err, resp, status){
                          if(err){
                            console.log(err);
                          }else{
            
                            console.log(resp);

                            res.render('index', {error_from_csv: false});
            
                          }
                        
                      });
        
                    //console.log(records);
                })
            );
        }
    });
    
    /*

    */
    


    /*if(req.query.firstname == undefined || req.query.firstname == null){
        res.render('index', {error: true, error_message: "Missing filed while adding bank account" , result: null});
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
                console.log(err);
                error_message = err;
              }else{

                result = resp._source;
                was_added = true;

              }

              res.render('add', {was_added: was_added, error: error, error_message: error_message, result: result});
          });

    } */
    
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
                console.log(err);
                error_message = err;
              }else{

                result = resp._source;
                was_edited = true;

              }

              res.render('edit', {was_edited: was_edited, error: error, error_message: error_message, result: result});
          });

    } 
    
});

//GET specific bank by id
server.get('/bank/', (req, res) => {
    var bank;
    client.search({
      index: 'bank',
      size: 5,
      }, function(err, resp, status){
        if(err){
          console.log(err);
        }else{
          bank = resp.hits.hits;
          console.log('found the requested document', resp);
          if (!bank){
            return res.status(400).send({
              message: 'bank is not found for id ${req.params.id}'
            });
          }
          return res.json({
            error: false,
            body: {
                message: 'Get bank calls for id ${req.params.id} succeeded',
                bank: bank
            }
          });
        }
    });
   })


server.listen(8082);

console.log('Server running at http://127.0.0.1:8082/');



/*

var workouts = [
  {
    id: 1,
    type: 'weight',
    duration: 45,
    date: '02/12/2020'
  },
  {
      id: 2,
      type: 'weight',
      duration: 49,
      date: '01/12/2020'
    }
]

//POST specific workout doc
server.post('/workout', (req, res) => {
  if(!req.body.id){
    return res.status(400).send({
      message: 'Id is required'
    });
  }
  client.index({
    index: 'workout',
    type: 'mytype',
    id: req.body.id,
    body: req.body
  }, function(err, resp, status){
    if(err){
      console.log(err);
    }else{
      return res.status(200).send({
        message: 'POST workout call succeeded'
      })
    }
  });
})



//GET specific shakespeare by id
server.get('/shakes/:id', (req, res) => {
 var shakespeare;
 client.get({
   index: 'shakespeare',
   type: '_doc',
   id: req.params.id
   }, function(err, resp, status){
     if(err){
       console.log(err);
     }else{
       shakespeare = resp._source;
       console.log('found the requested document', resp);
       if (!shakespeare){
         return res.status(400).send({
           message: 'shakespeare is not found for id ${req.params.id}'
         });
       }
       return res.status(200).send({
         message: 'Get shakespeare calls for id ${req.params.id} succeeded',
         shakespeare: shakespeare
       });
     }
 });
})



*/