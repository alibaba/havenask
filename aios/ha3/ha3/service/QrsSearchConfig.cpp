#include <ha3/service/QrsSearchConfig.h>

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, QrsSearchConfig);

QrsSearchConfig::QrsSearchConfig() {
    _resultCompressType = NO_COMPRESS;
}

QrsSearchConfig::~QrsSearchConfig() {
}

END_HA3_NAMESPACE(service);
