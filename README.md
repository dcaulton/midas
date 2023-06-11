# midas
dispatching system for a family of tenant applications

# Getting Started
1.  Make sure the following are installed locally: python3.8+, kafka
2.  pip install -r requirements.txt
3.  start up the faust app, similar to this:
    - faust  --datadir=/path_to_this_app/faust_data/1 -A dispatch.worker:app worker -l info --web-port=6066
4.  to verify it's working:
   - call it via http, using the endpoints defined in dispatch/worker.py
   - so post to something like http://localhost:6066/api/redact/v1/dispatch,
   - with a json payload like this:
      {
        "input": {"your":"data"},
        "operation": "test1"
      }
   - at command line you'll see a line like this:
     response data url is .....
   - verify that the data at response_data_url matches what you supplied with 'input'

