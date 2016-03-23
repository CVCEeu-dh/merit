var settings = require('./settings'),
    fs       = require('fs'),
    path     = require('path'),
    helpers  = require('./helpers'),
    async    = require('async'),
    _        = require('lodash'),
    clc        = require('cli-color'),

    parser   = require('xml2js').parseString;

var options = {};

var MAXQDA_DOCNAME = 'Document name', // csv column header are language specific
    MAXQDA_DOCGROUP = 'Document group',
    MAXQDA_BEGIN ='Begin', 
    MAXQDA_END ='End',
    MAXQDA_SEGMENT ='Segment',
    MAXQDA_CODE = 'Code',
    MAXQDA_PERTINENCE = 'Coverage %',
    MAXQDA_FILEPREFIX = 'loc_',
    maxqda = './Coded segments2016-03-21NEW.csv',//maxqda_codings2015-09-23.csv',
    geoloc = './Buildings-Streets-Cities-Provinces-220115.csv';


async.waterfall([
  function init(callback){

    fs.readdir('folia', function(err, files){
      options.folia = _(files)
        .filter(function(d) {
          return d.indexOf('folia.xml') != -1
        })
        .map(function(d){
          return {
            src: d,
            name: d.replace(/\.doc.*$/, '')
          }
        }).value()

      callback(null, options);
    })
    
  },
  function getMaxQDACSV(options, callback){
    options.source = maxqda;
    options.delimiter = ';'
    callback(null, options);
  },
  helpers.csv.parse,
  function readcsv(options, callback){
    // console.log(_.first(options.data), _.first(options.folia));
    // return;
    // options.data contains a list of :
    // { Farbe: '',
    // Kommentar: '',
    // Dokumentgruppe: 'Civilian',
    // Dokumentname: '19440917Herdenking2009Tineke',
    // Code: 'EVENTS\\Bombing other (Civilians)',
    // Anfang: '6',

    // 1. remap options.data on document name
    options.docs = _(options.data).groupBy(MAXQDA_DOCNAME).value();
    console.log('*',clc.blackBright(' MAX QDA annotated documents: ',_.keys(options.docs).length, '- sample:'), _.take(_.keys(options.docs), 2));

    // for each folia document, get the tags coming from options.docs
    callback(null, options);
  },
  function getGeolocCSV(options, callback){
    options.source = geoloc;
    options.delimiter = ';'
    callback(null, options);
  },
  helpers.csv.parse,
  function readcsv(options, callback){
    // console.log('LOC: ',_.take(options.data, 2));
    options.knownLocations = _(options.data).groupBy('document').value();
    console.log('*',clc.blackBright(' Location annotated documents: ',_.keys(options.knownLocations).length, '- sample:'),_.take(_.keys(options.knownLocations), 2));
    callback(null, options);
  },


  function crispr(options, callback){
    console.log('*', clc.blackBright(" Now let's open xml files one by one "));
    options.missingLocations = [];
    options.missingFoliaMaxQDAnames = [];

    var q = async.queue(function(folia, nextFolia) {
      console.log('+ ', clc.blackBright(' reading', clc.cyanBright(folia.name),'- remaining:'), q.length());
      async.waterfall([
        function readFolia(next) {
          fs.readFile(path.join('folia', folia.src), 'utf8', next); // err, contents
        },
        function parseFolia(xml, next){
          parser(xml, next);
        },
        function flattenFolia(xml, next) {
          // remap paragraph to be merged with MaxQDA, flatten down to SENTENCE LEVEL useful from FoLiA
          var sentences = _(xml.FoLiA.text[0].p)   
            .map(function(paragraph) {
              return paragraph.s.map(function(sentence){
                // extract
                return {
                  name: folia.name,
                  s: sentence.$['xml:id'].replace(/^.*\.(\d+)$/, function(m,d){return d}),
                  p: paragraph.$['xml:id'].replace(/^.*\.(\d+)$/, function(m,d){return d}),
                  sid: sentence.$['xml:id'],
                  pid: paragraph.$['xml:id'],
                  text: sentence.t.join(''),
                  locations: _(sentence.entities[0].entity)
                    .compact()
                    .filter(function(d){
                      // console.log(d, d.$)
                      return d.$.class == 'loc'
                    })
                    .map('wref[0].$')
                    .value(),
                  people: _(sentence.entities[0].entity)
                    .compact()
                    .filter(function(d){
                      return d.$.class == 'per'
                    })
                    .map('wref[0].$')
                    .value(),
                  entities: sentence.entities,
                  // dependencies: sentence.dependencies,
                  // keys: _.keys(sentence)
                };
              });
            })
            .flatten()
            .value();
          next(null, sentences);
        },

        function enrichWithMaxQda(sentences, next) {
          
          var tagpattern = /<\/*[LOC\/\\]+>/g;
          if(!options.docs[MAXQDA_FILEPREFIX +folia.name]){
            console.log(clc.magentaBright('NOT FOUND IN MAXQDA'), MAXQDA_FILEPREFIX +folia.name)
            options.missingFoliaMaxQDAnames.push({
              folia: folia.name,
              maxqda: MAXQDA_FILEPREFIX +folia.name
            });
            // throw 'not found'
          } else {
            console.log('   ', clc.greenBright('v'),clc.blackBright(' enrichWithMaxQda'), MAXQDA_FILEPREFIX +folia.name)
          }
          
          var _sentences = _(sentences)
              .map(function(s){
                
                if(s.p == 11 && s.s == 2){
                  console.log(clc.cyanBright(s.sid, s.p), clc.blackBright(s.text));
                }
                // get paragraph
                var segments = _(options.docs[MAXQDA_FILEPREFIX +folia.name])
                  .filter(function(d) {
                    if(s.p == 11 && s.s == 2){
                      console.log(s.text);
                      console.log(clc.blackBright(d[MAXQDA_SEGMENT].replace(tagpattern,'')))
                    }
                    var replaced = d[MAXQDA_SEGMENT].replace(tagpattern,'');

                    if(replaced.indexOf(s.text) !== -1)
                      if(s.p == 11 && s.s == 2){
                        console.log('Found', replaced)
                      }
                    // console.log('hey there', s.p,d[MAXQDA_BEGIN], s.p == d[MAXQDA_BEGIN])
                    // if((s.p == d[MAXQDA_BEGIN] || s.p == d[MAXQDA_END]))
                    // console.log('hey there')
                    // if there is an exact match and the match is long enough
                    if(d[MAXQDA_SEGMENT].length > 10 && (d[MAXQDA_SEGMENT].replace(tagpattern,'').indexOf(s.text) !== -1 || s.text.indexOf(d[MAXQDA_SEGMENT].replace(tagpattern,'')) !== -1))
                      return true;
                    var has_matches = (s.p == d[MAXQDA_BEGIN] || s.p == d[MAXQDA_END]) && (d[MAXQDA_SEGMENT].replace(tagpattern,'').indexOf(s.text) !== -1 || s.text.indexOf(d[MAXQDA_SEGMENT].replace(tagpattern,'')) !== -1)
                    
                    return has_matches;
                    // d[MAXQDA_SEGMENT].replace(/<\/*LOC>/g,'').indexOf(s.text) !== -1

                  })
                  .map(function(d){
                    return {
                      code: d[MAXQDA_CODE],
                      segment: d[MAXQDA_SEGMENT].replace(tagpattern,''),
                      group: d[MAXQDA_DOCGROUP],
                      pertinence: d[MAXQDA_PERTINENCE] //'Abdeckungsgrad %'],
                      // keys: _.keys(d)
                    }
                  })
                  .value();
                
                // if(segments.length > 1 )
                //   console.log(s.text, segments);
                s.matches = segments;
                s.group = segments.length? _.first(segments).group : undefined,
                s.s = parseInt(s.s);
                s.p = parseInt(s.p);
                return s;
              }).value();
          // throw 'stop'

          // console.log(sentences, options.docs[folia.name])
          next(null, _sentences);
        },

        //
        // every sentence having a location
        function enrichWithKnownLocation(sentences, next){//next(null, sentences);return;
          console.log(' ---------- ', 'enrichWithKnownLocation', sentences.length);
          if(!options.knownLocations[folia.name])
            console.log("Cant't find",clc.magentaBright(folia.name), 'in known locations');
          var _sentences = _(sentences)
            .map(function(s) {
              s.locations = _(s.locations).map(function(loc) {

                var knownlocation = _(options.knownLocations[folia.name])
                  .find(function(_loc){
                    // return if lice_en_Koos_Visser_uit_Kampen.p.6.s.1.w.10 is contained in <wref id="Alice_en_Koos_Visser_uit_Kampen.p.6.s.1.w.10" t="Oosterbeek"/> A
                    // Herdenking2009Tineke.p.1.s.3.w.9

                    return _loc.wrefs.indexOf(loc.id) != -1


                  });
                if(!knownlocation) {
                  // console.log(options.knownLocations[folia.name])
                  // throw 'err'
                  var uncertain = _(options.knownLocations[folia.name])
                    .find(function(_loc){
                      return _loc['straatnaam (match)'] == loc.t
                    })

                  if(uncertain) {
                    return _.assign(loc, {
                      partial: true,
                      place: _.compact([uncertain.plaats, uncertain.provincie]).join(', ')
                    });
                    
                  }
                  options.missingLocations.push({
                    name: s.name, sid: s.sid, p:s.p, s:s.s,text: s.text, place: loc.t
                  });
                  return '';
                }

                // console.log(loc)
                return _.assign(loc, {
                  place: _.compact([knownlocation.plaats, knownlocation.provincie]).join(', ')
                });
              }).compact().value();
              return s;
            })
            .compact() // get rid of empty stuff
            .value();

          next(null, sentences);
        },

        /*
          Call geocoding api (only for location having a "place")
        */
        function enrichWithGeocoding(sentences, next) { // next(null, sentences); return;
          var _q = async.queue(function(sentence, nextSentence){
            
            
            if(!sentence.locations.length){
              nextSentence();
              return;
            }
            
            // for the sentences having a place,
            async.series(sentence.locations.map(function(location) {
              return function(_next) {
                var address = location.place.indexOf(location.t) != -1? location.place: [location.t, location.place].join(', ');
                // console.log(address);
                helpers.cache.read({
                  namespace: 'services',
                  ref: 'geocoding:' + address
                }, function (err, contents) {
                  if(contents) {
                    // console.log('    from cached element');
                    _next(null, contents);
                    return;
                  }
                  helpers.geocoding({
                    address: address
                  }, function(err, results) {
                    if(err)
                      _next(err);
                    else 
                      helpers.cache.write(JSON.stringify(results), {
                        namespace: 'services',
                        ref: 'geocoding:' + address
                      }, function(err) {
                        _next(null, results);
                      });

                  });
                })
              }
            }), function(err, results) {
              if(err) {
                _q.kill();
                next(err);
              } else {
                result = _.flatten(results);
                // console.log(result)
                for(var i in result) {
                  if(result[i].geometry)
                    _.assign(sentence.locations[i], result[i].geometry.location, {query: result[i]._query});
                }
                setTimeout(function() {
                  nextSentence();
                }, 5);
              }
            })
            

          }, 1);
          _q.push(sentences);
          _q.drain = function() {
            next(null, sentences);
          }
        },

        function writeJson(sentences, next){
          console.log('writing file', path.join('folia', folia.name + '.json'), 'remaining', q.length());
          fs.writeFile(path.join('folia', folia.name + '.json'), JSON.stringify(sentences, null, 2), 'utf8', next);
        }
      ], function(err, sentences) {
        if(err){
          q.kill();
          callback(err);
        }
        nextFolia();
      });
    });

    //q.push(_.filter(options.folia, {name: 'Alice_en_Koos_Visser_uit_Kampen'}));
    // q.push(_.filter(options.folia, {name: 'VERHAAL_VAN_HORST_WEBER'}));
    // q.push(options.folia)
    q.push(_.filter(options.folia, {name: 'Interview_met_Dhr_Nijland'}));
    // q.push(_.filter(options.folia, {name: 'Veldhuizen_fam._Brieven'}))
    // q.push(_.filter(options.folia, {name: 'aanvullingen_op_Verhaal_Theo_Verbaars'}))
    q.drain = function() {
      callback(null, options);
      
      
    }
    
  },

  function writeReportMissinglocations(options, callback) {
    fs.writeFile('missingLocations.json', JSON.stringify(options.missingLocations, null, 2), 'utf8', function(){
        callback(null, options);
      });
  },
  function writeReportMissingFoliaMaxQDAnames(options, callback) {
    fs.writeFile('missingFoliaMaxQDAnames.json', JSON.stringify(options.missingFoliaMaxQDAnames, null, 2), 'utf8', function(){
        callback(null, options);
      });
  }
], function(err) {
  if(err)
    console.log(err)
  else
    console.log('ok')
});