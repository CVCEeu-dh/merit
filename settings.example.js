/*
  settings file
*/
module.exports = {
  geocoding: { // google geocoding api
    endpoint: 'https://maps.googleapis.com/maps/api/geocode/json',
    key: 'YOUR KEY HERE'
  },

  paths:{
    cache:{
      services: './cache'

    }
  }
};