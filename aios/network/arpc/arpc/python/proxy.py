#!/bin/evn python
from rpc_impl import ANetRpcImpl

class Proxy( object ):
    class _Proxy( object ):
        def __init__( self, stub ):
            self._stub = stub

        def __getattr__( self, key ):
            def call( method, controller, request ):
                class callbackClass( object ):
                    def __init__( self ):
                        self.response = None
                    def __call__( self, response ):
                        self.response =  response
                callback = callbackClass()
                method( controller, request, callback )
                return callback.response
            return lambda controller, request: call( getattr( self._stub, key ), controller, request )

    def __init__( self, *stubs ):
        self._stubs = {}
        for s in stubs:
            self._stubs[ s.GetDescriptor().name ] = self._Proxy( s )
    
    def __getattr__( self, key ):
        return self._stubs[ key ]
    
