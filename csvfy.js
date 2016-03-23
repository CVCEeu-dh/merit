/*
  This script transforms every json files in `settings.paths.folia path` in custom csv files.
  The csv files are then stored in the same `settings.paths.folia path`
*/
var settings = require('./settings'),
    fs       = require('fs'),
    path     = require('path'),
    helpers  = require('./helpers'),
    async    = require('async'),
    _        = require('lodash'),

    json2csv = require('json2csv'),
    clc      = require('cli-color'),

    options  = {};


async.waterfall([
  function init(callback){
    console.log(clc.magentaBright('merit'));
    fs.readdir('folia', function(err, files){
      options.folia = _(files)
        .filter(function(d) {
          return d.indexOf('.json') != -1
        })
        .map(function(d){
          return {
            src: d,
            name: d.replace(/\.json*$/, '')
          }
        })
        .value()
      callback(null, options);
    });
  },

  function csvfy(options, callback){
    console.log('found', clc.magentaBright(options.folia.length), 'json files, e.g:')
    console.log(_.take(options.folia, 2))
    options.merged = [];

    var q = async.queue(function(folia, nextFolia){
      // read file content
      console.log(clc.blackBright('   file:', clc.cyanBright(folia.src), 'remaining:'), q.length())
      async.waterfall([
        function getJsondata(next){
          fs.readFile(path.join('folia', folia.src), 'utf8', next);
        },
        function parse(content, next) {
          next(null, _(JSON.parse(content)).map(function(sentence){
            var combinations = [
              {
                name: sentence.name,
                s: sentence.s,
                p: sentence.p,
                text: sentence.text,
                place: '',
                lat: '',
                lng: '',
                is_known: '',
                group: '',
                code : ''
              }
            ];

            if(sentence.locations.length >0) {
              combinations = sentence.locations.map(function(location){
                return {
                  name: sentence.name,
                  s: sentence.s,
                  p: sentence.p,
                  text: sentence.text,
                  place: location.query,
                  lat: location.lat,
                  lng: location.lng,
                  is_known: location.partial? 0: 1,
                  group: sentence.group,
                  code : ''
                };
              });
            };

            if(sentence.matches.length > 0)
              combinations = _.map(combinations, function(d){
                d.code = _.map(sentence.matches, 'code').join('||');
                return d
              });

            return combinations;
          })
            .flatten()
            .compact()
            .value()
          );
        },
        function transform(data, next){
          options.merged = options.merged.concat(data)
          json2csv({ 
            data: data, 
            fields: [
              "name", "s", "p", "text", "place", "lat", "lng", "is_known", "group", "code"
            ]
          }, next);
        },
        function writeCsv(csvdata, next){
          fs.writeFile(path.join('folia', folia.name + '.csv'), csvdata, 'utf8', next);
        }
      ], function(err) {
        if(err){
          q.kill();
          callback(err);
        } else {
          nextFolia()
        }
      });

    }, 1);
    // q.push(_.take(options.folia,2))
    q.push(options.folia)
    q.drain = function(){
      json2csv({ 
        data: options.merged, 
        fields: [
          "name", "s", "p", "text", "place", "lat", "lng","is_known", "group", "code"
        ]
      }, function(err, csvdata){
        fs.writeFile(path.join('folia', 'merged.csv'), csvdata, 'utf8', callback);
      });
      
    }
  }
], function(err){
  if(err)
    console.log(err)
  else 
    console.log(clc.greenBright('Done!'));
});