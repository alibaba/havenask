import time, logging, json
import http_util

logger = logging.getLogger(__name__)

class C2GatewayClient:
    directProxyURLPrefix = '/directproxy'

    def __init__(self, gatewayRoot):
        fields = gatewayRoot.split(";")
        for field in fields:
            kvs = field.split("=")
            if len(kvs) == 2:
                if kvs[0] == "host":
                    self.spec = kvs[1]
                if kvs[0] == "platform":
                    self.platform = kvs[1]
                if kvs[0] == "clusterid":
                    self.clusterId = kvs[1]

    def submit_application(self, app_desc):
        url = self._operation_url('/hippos/operation')
        logger.info('url:' + url)
        metaInfo = {'clusterId': self.clusterId, 'platform': self.platform}
        body = {'op': 'POST', 'Path': '/ClientService/submitApplication', 'value': {'applicationDescription': app_desc}, 'metaInfo': metaInfo}
        js = http_util.post(url, body)
        return js

    def update_application(self, app_desc):
        url = self._operation_url('/hippos/operation')
        metaInfo = {'clusterId': self.clusterId, 'platform': self.platform}
        body = {'op': 'POST', 'Path': '/ClientService/updateAppMaster', 'value': {'applicationDescription': app_desc}, 'metaInfo': metaInfo}
        js = http_util.post(url, body)
        return js

    def stop_application(self, name):
        url = self._operation_url('/hippos/operation')
        metaInfo = {'clusterId': self.clusterId, 'platform': self.platform}
        body = {'op': 'POST', 'Path': '/ClientService/stopApplication', 'value': {'applicationId': name}, 'metaInfo': metaInfo}
        js = http_util.post(url, body)
        return js

    def get_appsummary(self, name):
        return self._get_appcommon(name, '/ClientService/getAppSummary')

    def get_appstatus(self, name):
        return self._get_appcommon(name, '/ClientService/getAppStatus')

    def get_appdescription(self, name):
        return self._get_appcommon(name, '/ClientService/getAppDescription')

    def get_allappsummary(self):
        url = 'http://' + self.spec + '/api/hippos/' + self.clusterId + '/applications?platform=' + self.platform
        rt = http_util.get(url)
        return rt

    def get_old_appstatus(self, name):
        url = 'http://' + self.spec + '/api/hippos/' + self.clusterId + '/applications/' + name + '?platform=' + self.platform
        rt = http_util.get(url)
        return rt

    def get_hippoinfo(self):
        url = 'http://' + self.spec + '/api/hippos/' + self.clusterId
        rt = http_util.get(url)
        return rt

    def _get_appcommon(self, name, path):
        url = self._operation_url('/hippos/operation')
        metaInfo = {'clusterId': self.clusterId, 'platform': self.platform}
        body = {'op': 'POST', 'path': path, 'value': {'applicationId': name}, 'metaInfo': metaInfo}
        js = http_util.post(url, body)
        return js

    def _operation_url(self, uri):
        return 'http://' + self.spec + '/api/v1' + uri
