"""
AsyncBoto

Thomas Parslow 2011
tom@almostobsolete.net

Make Boto operate in an Asynchronous way using Tornado.

I've only tested with SimpleDB so far (and then only a little bit)
but you should be able to do the same with all the other types of
connection in Boto.

THIS IS RATHER EARLY RIGHT NOW, IT'S NOT PROPERLY TESTED AND IT MAY
JUST BE A STUPID IDEA ALTOGETHER. I JUST WANT TO GET SOME FEEDBACK.
DON'T BE STUPID AND USE THIS FOR SOMETHING IMPORTANT. THANKS :)

"""
import boto.sdb.connection
import tornado.httpclient
import tornado.ioloop
import urlparse
import functools
import hashlib

class AsyncCallInprogress(Exception):
    pass

class AsyncHttpResponse(object):
    """
    Emulates the parts of httplib's HTTPResponse that Boto uses.
    """
    def __init__(self, status, reason, body, headers):
        self.status = status
        self.reason = reason
        self.body = body
        self.headers = headers

    def read(self):
        return self.body
    
    def getheader(self, name):
        return self.headers.get(name,None)

class AsyncHttpConnection(object):
    """
    This class replaces the httplib HTTPConnection used by boto. Only
    the bits actually used by Boto are provided (at least I hope they
    are!).

    When a request is actually made (which happens in "getresponse")
    the cache of previous requests (for this particular connection,
    which would live no longer than a single boto async call) and if
    one is found it returns it right away.

    If no request is found one is started and a AsyncCallInprogress
    exception is raised. This causes the call in progress to be
    stopped for the moment. When the response actually comes through
    the Boto call is made again, but this time when it gets to the bit
    where it calls getresponse a response is ready and waiting.

    This cycle could happen a few times during a an async call into
    Boto but during the final call through all the requests will be in
    the cache and thus the call will complete without raising an
    AsyncCallInprogress.
    """
    def __init__(self, aws_connection, fn, callback, errback):
        self.aws_connection = aws_connection

        self.fn = fn
        self.callback = callback
        self.errback = errback
        
        self.host = None
        self.is_secure = None

        self.response_cache = {}

    def _get_request_sig(self):
        """
        request_sig is a tuple that can be used to look up a request.
        It consists of the request details minus the timestamp and the
        signature (which will differ if they're called even a tiny bit
        of time apart from eachother).

        The request sig is based on the current values of the member
        variables. 
        """
        path_minus_query,query = self.path.split("?",1)
        query = [(k,tuple(v)) for k,v in urlparse.parse_qs(query).items() if k not in ["Timestamp","Signature"]]
        query.sort()
        # Hash this so that large body's aren't kept around when not
        # needed
        return hashlib.md5(repr((self.is_secure, self.host, self.method,
                            path_minus_query, tuple(query), self.data,
                            tuple(sorted(self.headers.items()))))).hexdigest()
        
    def request(self, method, path, data, headers):
        """
        In the httplib version this would actually make the request,
        but here we just store the params ready for use in the
        getresponse method.
        """
        self.method = method
        self.path = path
        self.data = data
        self.headers = headers

    def _callback(self, request_sig, tornado_response):
        """
        The Tornado httpclient callback. Is partially applied to the
        request_sig so the response can be stuffed into response_cache.
        """
        response = AsyncHttpResponse(
            tornado_response.code, "???", tornado_response.body, tornado_response.headers)
        self.response_cache[request_sig] = response
        #self.aws_connection._async_http_connection = self
        #retry the call, with the response in place this time
        self.aws_connection._call_async(self.fn, callback=self.callback, errback=self.errback, async_http_connection=self)
        
    def getresponse(self):
        """
        Checks response_cache, if we already have a response for this
        request then return it otherwise start the Async call and
        raise AsynCallInProgress.
        """
        request_sig = self._get_request_sig()
        if request_sig in self.response_cache:
            # We already made the request and got the response, carry
            # on!
            return self.response_cache[request_sig]
        # Ok, we need to make a request
        http_client = tornado.httpclient.AsyncHTTPClient()
        if self.is_secure:
            schema = "https"
        else:
            schema = "http"
        url = "%s://%s%s" % (schema, self.host, self.path)
        request = tornado.httpclient.HTTPRequest(url,self.method, self.headers, self.data or None)
        http_client.fetch(request, functools.partial(self._callback,request_sig))
        raise AsyncCallInprogress


class AsyncConnectionMixin(object):
    """
    Mixin to replace get_http_connection and put_http_connection in a
    subclass of AWSAuthConnection from Boto to create an Async version
    of a connection class.

    All calls to methods in the new Async version must be wrapped in
    call_async calls to make then operate asynchronously. For example:

      sdb_conn.call_async(
        lambda : sdb_conn.get_domain("my_test_domain").get_attributes("hello"),
          callback=hello)

    call_async sets up AsyncHttpConnection with the callback and also
    traps the AsyncCallInProgress exception.

    Exceptions are trapped and send to the "errback" callback.
    """
    def call_async(self, fn, callback, errback=lambda e : None):
        return self._call_async(fn, callback, errback)
        
    def _call_async(self, fn, callback, errback, async_http_connection=None):
        self._async_http_connection = async_http_connection
        try:
            if not self._async_http_connection:
                self._async_http_connection = AsyncHttpConnection(self, fn, callback,errback)
            try:
                ret = fn()
            except AsyncCallInprogress:
                pass
            except Exception, e:
                tornado.ioloop.IOLoop.instance().add_callback(lambda : errback(e))
            else:
                # When a call finally succeeds without raising
                # AsyncCallInprogress we then need to pass control to the
                # callback.
                #
                # This could also happen first time if the call doesn't
                # involve any HTTP requests. But call_async would still
                # return right away and have the callback called on the
                # next interation of the IOLoop
                tornado.ioloop.IOLoop.instance().add_callback(lambda : callback(ret))
        finally:
            del self._async_http_connection
        
    def get_http_connection(self, host, is_secure):
        """
        This is called to get an HTTP connection from the pool. This
        is the point at which we inject our replacement http connection
        """
        if hasattr(self, "_async_http_connection"):
            self._async_http_connection.host = host
            self._async_http_connection.is_secure = is_secure
            return self._async_http_connection
        else:
            # This hasn't been called from within an async_call so
            # just allow it to do a normal synchronous call
            super(AsyncConnectionMixin, self).get_http_connection(host,is_secure)

    def put_http_connection(self, *args, **kwargs):
        if not hasattr(self, "_async_http_connection"):
            super(AsyncConnectionMixin, self).put_http_connection(*args, **kwargs)
        
            
class AsyncSDBConnection(boto.sdb.connection.SDBConnection,AsyncConnectionMixin):
    pass


if __name__ == "__main__":
    from tornado.options import define, options
    define("aws_access_key_id", type=str)
    define("aws_access_key_secret", type=str)
    tornado.options.parse_command_line()
    
    sdb_conn = AsyncSDBConnection(options.aws_access_key_id, options.aws_access_key_secret)
    def callback2(ret):
        print "Return from get was: ", ret
        tornado.ioloop.IOLoop.instance().stop()
    def callback1(ret):
        print "Return from put was:", ret
        sdb_conn.call_async(lambda : sdb_conn.get_domain("mytest").get_attributes("boom"), callback=callback2)
        
    sdb_conn.call_async(lambda : sdb_conn.create_domain("mytest").put_attributes("boom", {"hello": "goodbye"}), callback=callback1)

    # Test errback
    def callback3(ret):
        assert False, "We were expecting an error!"
    def errback3(exception):
        print "Exception received: ", exception
    sdb_conn.call_async(lambda : sdb_conn.get_domain("i_do_not_exist"), callback=callback3, errback=errback3)

    tornado.ioloop.IOLoop.instance().start()
