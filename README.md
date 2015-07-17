dataunity-datastructdef
=======================

Creates schema metadata information about a file - like the columns and their data types.

Job Start Message format
------------------------
###Stomp headers
- subscription
- message-id: a new GUID string
- destination: topic to send message to
- content-type: application/json
- correlation-id: correlation id so the response can be matched to this job start message

###Stomp body
JSON string containing dictionary with properties:
- dataUnity_base_url: the base url that will be used for new IRIs for metadata info
- path: the local path to the file that will be analysed
- encoding: the character encoding of the file

Job Reply Message format
------------------------
###Stomp headers
- subscription
- message-id: a new GUID string
- destination: topic to send reply message to
- content-type: text/turtle
- correlation-id: correlation id so the response can be matched to this job start message

###Stomp body
RDF string in turtle format with a graph detailing the file meta data.
