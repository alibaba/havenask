#include <ha3/config/QrsChainInfo.h>

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(qrs, QrsChainInfo);

ProcessorNameVec QrsChainInfo::getPluginPoint(const std::string &pointName) const 
{
    QrsPluginPoints::const_iterator it = _pluginPoints.find(pointName);
    if (it != _pluginPoints.end()) {
        return it->second;
    } else {
        ProcessorNameVec nullProcessorNameVec;
        return nullProcessorNameVec;
    }
}

void QrsChainInfo::addPluginPoint(const std::string &pointName, 
                                  const ProcessorNameVec &pointProcessors) 
{
    if (_pluginPoints.find(pointName) == _pluginPoints.end()) {
        _pluginPoints[pointName] = pointProcessors;
    } else {
        HA3_LOG(WARN, "Add qrs plugin fail %s", pointName.c_str());
    }
}

void QrsChainInfo::addProcessor(const std::string &pointName,
                                const std::string &processorName) 
{
    _pluginPoints[pointName].push_back(processorName);
}

void QrsChainInfo::Jsonize(JsonWrapper& json) {
    json.Jsonize("chain_name", _chainName);
    json.Jsonize("plugin_points", _pluginPoints, QrsPluginPoints());
}
END_HA3_NAMESPACE(config);

