#include <ha3/common/GlobalIdentifier.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, GlobalIdentifier);

void ExtraDocIdentifier::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(primaryKey);
    dataBuffer.write(fullIndexVersion);
    dataBuffer.write(indexVersion);
    dataBuffer.write(ip);
}

void ExtraDocIdentifier::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(primaryKey);
    dataBuffer.read(fullIndexVersion);
    dataBuffer.read(indexVersion);
    dataBuffer.read(ip);
}
//////////////////////////////////////////////////////////////////////////////
void GlobalIdentifier::serialize(autil::DataBuffer &dataBuffer) const {
    HA3_LOG(DEBUG, "GlobalIdentifier serialize.");
    dataBuffer.write(_docId);
    dataBuffer.write(_hashId);
    dataBuffer.write(_clusterId);
    dataBuffer.write(_extraDocIdentifier);
    dataBuffer.write(_pos);
}

void GlobalIdentifier::deserialize(autil::DataBuffer &dataBuffer) {
    HA3_LOG(DEBUG, "GlobalIdentifier deserialize.");
    dataBuffer.read(_docId);
    dataBuffer.read(_hashId);
    dataBuffer.read(_clusterId);
    dataBuffer.read(_extraDocIdentifier);
    dataBuffer.read(_pos);
}

END_HA3_NAMESPACE(common);
