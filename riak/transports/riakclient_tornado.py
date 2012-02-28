"""
Copyright 2012 Ivan Blinkov <ivan@blinkov.ru>

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
from riak.transports.connection import cm_using
from riak.transports.http import RiakHttpTransport
from tornado.httpclient import AsyncHTTPClient
from tornado.web import asynchronous
from greenlet import greenlet, getcurrent
from functools import wraps
from logging import warning

def async(wrapped_method):
    """
    The async decorator allows to use asynchronous calls to Riak
    from get/post/put/delete methods of Tornado's RequestHandlers.
    """
    @asynchronous
    @wraps(wrapped_method)
    def wrapper(self, *args, **kwargs):
        def greenlet_base_func():
            wrapped_method(self, *args, **kwargs)
            self.finish()

        gr = greenlet(greenlet_base_func)
        gr.switch()

    return wrapper

class HTTPResponse(object):
    """
    This HTTPResponse mocks the corresponding objects, expected in RiakHttpTransport,
    using the data from Tornado's HTTPResponse passed as argument to constructor.
    """
    def __init__(self, response):
        self.status = response.code
        self.headers = response.headers
        self.body = response.body

    def getheaders(self):
        return self.headers.items()

    def read(self):
        return self.body

    def close(self):
        pass

class TornadoConnection(object):
    """
    Mocks httplib.HTTPConnection using AsyncHTTPClient to make request and
    creating greenlet to hold context and yield control back to IOLoop until
    the response is received.
    """
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = AsyncHTTPClient()

    def request(self, method, uri, body, headers):
        self.method = method
        self.uri = uri
	if body: self.body = body
	else: self.body = None
        self.headers = headers

    def getresponse(self):
        gr = getcurrent()
        def _callback(response):
            gr.switch(response)
        self.client.fetch('http://%s:%s%s' % (self.host, self.port, self.uri), \
                          _callback, method = self.method, headers = self.headers, body = self.body)
        return HTTPResponse(gr.parent.switch())


class RiakTornadoTransport(RiakHttpTransport):
    """
    RiakTornadoTransport overrides the default ConnectionManager in order to use
    AsyncHTTPClient instead of standard httplib.
    """
    default_cm = cm_using(TornadoConnection)
