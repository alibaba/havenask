import urllib2, urllib, httplib, socket, errno
import json, logging, time

logger = logging.getLogger(__name__)

def get_raw(url):
    return _doreq(url, timeout=90)

def get(url):
    resp = _doreq(url, timeout=90)
    if resp == '':
        return None
    return json.loads(resp)

def post(url, data, headers={}, timeout=90):
    if not headers.has_key('Content-Type'):
        headers['Content-Type'] = 'application/json'
    resp = _doreq(url, timeout=timeout, body=json.dumps(data), headers=headers, request_method='POST')
    if resp == '':
        return None
    result = json.loads(resp)
    return result

def simple_post(url, data, headers={}, timeout=90):
    if not headers.has_key('Content-Type'):
        headers['Content-Type'] = 'application/json'
    resp = _doreq(url, timeout=timeout, body=json.dumps(data), headers=headers, request_method='POST')
    return resp

def put(url, data, headers={}):
    if not headers.has_key('Content-Type'):
        headers['Content-Type'] = 'application/json'
    resp = _doreq(url, timeout=90, body=json.dumps(data), headers=headers, request_method='PUT')
    if resp == '':
        return None
    result = json.loads(resp)
    return result

def delete(url):
    resp = _doreq(url, timeout=90, request_method='DELETE')
    if resp == '':
        return None
    return json.loads(resp)

def _doreq(url, timeout, d_params={}, body=None, headers={}, request_method=""):
    if len(d_params) > 0:
        q = urllib.urlencode(d_params)
        url = url + '?' + q
    content = ''
    while True:
        try:
            start_at = time.time()
            request = urllib2.Request(url, body, headers)
            if request_method == "PUT":
                request.get_method = lambda: 'PUT'
            if request_method == "DELETE":
                request.get_method = lambda: 'DELETE'
            response = urllib2.urlopen(request, timeout=timeout)
            content = response.read()
            if (time.time() - start_at) * 1000 > 3000:
                logger.warn('http %s %s cost %d ms', request_method, url, 1000 * (time.time() - start_at))
            return content
        except urllib2.HTTPError, e:
            logger.error('request %s:%s failed, http error %s: %s' % (request_method, url, e, e.read()))
            break
        except httplib.HTTPException, e:
            logger.error('request %s http exception: %s', url, e)
            break
        except urllib2.URLError, e:
            logger.warn('request %s %s failed: %r' % (request_method, url, e))
            if e.errno == errno.EINTR: continue
            break
        except socket.timeout, e:
            logger.error('request %s %s failed, socket timeout %r' % (request_method, url, e))
            break
        except socket.error, e:
            logger.error('request %s failed, socket error %r' % (url, e))
            break
    return content