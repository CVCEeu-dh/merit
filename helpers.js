var settings   = require('./settings'),
    fs         = require('fs'),
    path       = require('path'),
    csv        = require('csv'),
    clc         = require('cli-color'),
    request    = require('request'),
    _          = require('lodash');

module.exports = {
  csv: {
    stringify: function(options, callback) {
      console.log(clc.yellowBright('\n   tasks.helpers.csv.stringify'));
      csv.stringify(options.records, {
        delimiter: options.delimiter || '\t',
        columns:   options.fields,
        header:    true
      }, function (err, data) {
        fs.writeFile(options.filepath,
           data, function (err) {
          if(err) {
            callback(err);
            return
          }
          callback(null, options);
        })
      });
    },
    /*
      REQUIRE an absolute or relative to this file task
    */
    parse: function(options, callback) {
      console.log(clc.yellowBright('\n   tasks.helpers.csv.parse'));
      if(!options.source) {
        return callback(' Please specify the file path with --source=path/to/source.tsv');
      }
      csv.parse(''+fs.readFileSync(options.source), {
        columns : true,
        delimiter: options.delimiter || '\t'
      }, function (err, data) {
        if(err) {
          callback(err);
          return;
        }
        console.log(clc.blackBright('   parsing csv file completed,', clc.magentaBright(data.length), 'records found'));
        options.data = data;
        callback(null, options);
      });
    }
  },


  cache: {
    /*
      
    */
    naming: function(options) {
      var md5 = require('md5');
      return path.join(settings.paths.cache[options.namespace], options.ref = md5(options.ref) + '.json')
    },
    write: function(contents, options, next) {
      console.log('    writing in cache...')
      if(_.isEmpty(settings.paths.cache[options.namespace]))
        next(IS_EMPTY)
      else
        fs.writeFile(module.exports.cache.naming(options), contents, next)
    },
    read: function(options, next) {
      if(_.isEmpty(settings.paths.cache[options.namespace]))
        next(IS_EMPTY)
      else
        fs.readFile(module.exports.cache.naming(options), 'utf8', function (err, contents) {
          if(err)
            next(err);
          else {
            try {
              next(null, JSON.parse(contents))
            } catch(e) {
              console.log(e)
              next(null, contents);
            }
          }
        })
    },
    unlink: function(options, next) {
      if(_.isEmpty(settings.paths.cache[options.namespace]))
        next(IS_EMPTY)
      else
        fs.unlink(module.exports.cache.naming(options), next);
    }
  },

  geocoding: function(options, next) {
    if(!settings.geocoding ||_.isEmpty(settings.geocoding.key)) {
      next(null, []);
      return;
    }

    // is it a file named ...
    request.get({
      url: settings.geocoding.endpoint,
      qs: _.assign({
        key: settings.geocoding.key
      }, options, {
        q: options.address
      }),
      json: true
    }, function (err, res, body) {
      if(err) {
        console.log('service geocoding failed')
        next(err);
        return;
      }
      
      // console.log(url)
      if(!body.results.length) {
        if(body.error_message) {
          console.log('service geocoding failed')
          next(body.error_message)
          return;
        } 
        next(null, []);
        return;
      };
      // adding name, fcl and country code as well: it depends on the level.
      next(null, _.take(body.results.map(function (result) {
        var name = result.formatted_address,
            fcl, 
            country;
        
        if(result.types.indexOf('continent') != -1) { 
          fcl = 'L';
        } else if(result.types.indexOf('country') != -1) {
          fcl = 'A';
        } else if(result.types.indexOf('locality') != -1) {
          fcl = 'P';
        } else {
          // console.log(result, options.address)
          // throw 'stop'
        }
        country = _.find(result.address_components, function (d){
          return d.types.indexOf('country') != -1
        });   
        
        if(!country){
          country = result.address_components[0]
        }
        if(!country){
          console.log(result)
          throw 'stop'
        }
        
        return _.assign(result, {
          _id:      result.place_id,
          _name:    name,
          _fcl:     fcl,
          _country: country.short_name,
          _query:   options.address,
        });
      }), 1));
      
      // if()
      
      // console.log(body.results[0].formatted_address, 'for', options.address);
      // console.log(body.results[0].address_components)

      // var country = _.find(body.results[0].address_components, function (d){
      //     return d.types[0] == 'country';
      //   }),
      //   locality =  _.find(body.results[0].address_components, function (d){
      //     return d.types[0] == 'locality';
      //   });
      // // the entity name
      // var name_partials = [];
      // if(locality && locality.long_name)
      //   name_partials.push(locality.long_name);
      // if(country && country.long_name)
      //   name_partials.push(country.long_name);
      // var name = name_partials.length? name_partials.join(', '): body.results[0].formatted_address;
      // console.log(body.results[0], name)
    })
  }
}