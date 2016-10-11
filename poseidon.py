import socket
import time

def retryLoop(n=1, wait=0):
    def decoratorFunc(func):
        def wrapped(*args, **kwargs):
            retry = 0
            exception = None
            while retry <= n:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    exception = e
                    time.sleep(wait)
                    retry += 1
            raise exception        
        return wrapped
    return decoratorFunc


class Kratos(object):
    
    def __init__(self, host='localhost', port=5000):
        self._host = host
        self._port = port

    def _intToByteArray(self, num):
        return [(num >> i & 0xff) for i in (24, 16, 8, 0)]

    @retryLoop(n=3, wait=3)
    def put(self, key, value):
        
        klen = len(key)
        rlen = len(value) + klen
        
        s = socket.socket()
        s.settimeout(10)
        try:
            s.connect((self._host, self._port))
            s.send(bytearray([0x0] + self._intToByteArray(rlen) + self._intToByteArray(klen)))
            s.send(bytearray(key + value))
        finally:
            s.close()
    
    @retryLoop(n=3, wait=3)
    def get(self, key):
        
        klen = len(key)
        s = socket.socket()
        
        try:
            s.connect((self._host, self._port))
            s.send(bytearray([0x1] + self._intToByteArray(klen)))
            s.send(bytearray(key))
            
            data = []
            buf = s.recv(1024)
            while buf:
                data.append(buf)
                buf = s.recv(1024)
            data = ''.join(data)
            return data[4:]
        finally:
            s.close()


if __name__ == '__main__':
    
    store = Kratos()
    
    for i in xrange(50000):
        key = 'hello-key-%d' % i
        value = '%d: Thank you for your interest in this question' % i
        store.put(key, value)
        print 'Written for the key - %s' % key
    
    for i in xrange(50000):
        key = 'hello-key-%d' % i
        value = store.get(key)
        print "Value is %s" % value
