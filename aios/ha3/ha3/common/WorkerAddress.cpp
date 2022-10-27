#include <ha3/common/WorkerAddress.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, WorkerAddress);

WorkerAddress::WorkerAddress(const string &address) {
    _address = address;
    size_t pos = _address.find(":");

    if (pos == string::npos) {
        _ip = address;
        _port = 0;
    } else {
        _ip = _address.substr(0, pos);
        _port = StringUtil::fromString<uint16_t>(_address.substr(pos + 1));
        assert(StringUtil::toString(_port) == _address.substr(pos + 1));
    }
}

WorkerAddress::WorkerAddress(const string &ip, uint16_t port) {
    _address = ip + ":" + StringUtil::toString(port);
    _ip = ip;
    _port = port;
}

WorkerAddress::~WorkerAddress() { 
}

END_HA3_NAMESPACE(common);
