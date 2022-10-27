#ifndef ISEARCH_QRSCHAINCONFIG_H
#define ISEARCH_QRSCHAINCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ProcessorInfo.h>
#include <ha3/config/QrsChainInfo.h>
#include <map>

BEGIN_HA3_NAMESPACE(qrs);

/** the data structure parsed from config file. */
struct QrsChainConfig {
    std::map<std::string, config::ProcessorInfo> processorInfoMap;
    std::map<std::string, config::QrsChainInfo> chainInfoMap;
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSCHAINCONFIG_H
