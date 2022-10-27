#ifndef ISEARCH_QRSCHAINCONFIGURATOR_H
#define ISEARCH_QRSCHAINCONFIGURATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/json.h>
#include <ha3/config/QrsConfig.h>
#include <ha3/qrs/QrsChainConfig.h>
#include <map>
#include <string>

BEGIN_HA3_NAMESPACE(qrs);

//tag in config file
#define PROCESSORS_TAG "processors"
#define PROCESSOR_NAME_TAG "processor_name"
#define PROCESSOR_MODULE_TAG "module_name"
#define PROCESSOR_PARAMETER_TAG "parameters"

#define CHAINS_TAG "chains"
#define CHAIN_NAME_TAG "chain_name"
#define CHAIN_POINTS_TAG "plugin_points"
#define CHAIN_POINT_TAG "plugin_point"
#define CHAIN_PROCESSOR_TAG "processors"


//allowed plugin point
#define BEFORE_PARSER_POINT "BEFORE_PARSER_POINT"
#define BEFORE_VALIDATE_POINT "BEFORE_VALIDATE_POINT"
#define BEFORE_SEARCH_POINT "BEFORE_SEARCH_POINT"


/** this class is to parse the 'QrsChainConfig' object from config file.*/
class QrsChainConfigurator
{
public:
    QrsChainConfigurator();
    ~QrsChainConfigurator();
public:
    bool parseFromFile(const std::string &configFile, 
                       QrsChainConfig &qrsConf);
    bool parseFromString(const std::string &configFileContent, 
                         QrsChainConfig &qrsConf);
private:
    bool parseProcessorInfos(autil::legacy::json::JsonArray &precessors, 
                             std::map<std::string, config::ProcessorInfo> &processorInfoMap);
    bool parseChainInfos(autil::legacy::json::JsonArray &chains, 
                         std::map<std::string, config::QrsChainInfo> &chainInfoMap);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSCHAINCONFIGURATOR_H
