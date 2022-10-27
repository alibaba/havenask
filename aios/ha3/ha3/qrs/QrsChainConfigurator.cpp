#include <ha3/qrs/QrsChainConfigurator.h>
#include <iostream>
#include <exception>
#include <autil/legacy/json.h>
#include <suez/turing/common/FileUtil.h>

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace suez::turing;

USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, QrsChainConfigurator);

QrsChainConfigurator::QrsChainConfigurator() { 
}

QrsChainConfigurator::~QrsChainConfigurator() { 
}

bool QrsChainConfigurator::parseFromFile(const string &configFile, 
                                 QrsChainConfig &qrsConf)
{
    string configFileContent = FileUtil::readFile(configFile);
    return parseFromString(configFileContent, qrsConf);
}

bool QrsChainConfigurator::
parseFromString(const string &configFileContent, QrsChainConfig &qrsConf) {
    try {
        Any any = ParseJson(configFileContent);
        JsonMap jsonConf = AnyCast<JsonMap> (any);

        if (jsonConf.find(PROCESSORS_TAG) != jsonConf.end() 
            && jsonConf.find(CHAINS_TAG) != jsonConf.end())
        {
            JsonArray processors = AnyCast<JsonArray>(jsonConf[PROCESSORS_TAG]);
            map<string, ProcessorInfo> &processorInfoMap = qrsConf.processorInfoMap;
            parseProcessorInfos(processors, processorInfoMap);

            JsonArray chains = AnyCast<JsonArray>(jsonConf[CHAINS_TAG]);
            map<string, QrsChainInfo> &chainInfoMap = qrsConf.chainInfoMap;
            parseChainInfos(chains, chainInfoMap);
        } else {
            HA3_LOG(ERROR, "Invalid qrs config: \n%s\n", configFileContent.c_str());
            return false;
        }
    } catch(exception &e) {
        HA3_LOG(ERROR, "cached a exception, msg: %s", e.what());
        return false;
    }
    return true;    
}

bool QrsChainConfigurator::parseProcessorInfos(JsonArray &processors, 
        map<string, ProcessorInfo> &processorInfoMap) 
{
    for (size_t i = 0; i < processors.size(); i++)
    {
        JsonMap pInfoJson = AnyCast<JsonMap>(processors[i]);
        string processorName = AnyCast<string>(pInfoJson[PROCESSOR_NAME_TAG]);
        string moduleName = AnyCast<string>(pInfoJson[PROCESSOR_MODULE_TAG]);

        JsonMap jsonParams = AnyCast<JsonMap>(pInfoJson[PROCESSOR_PARAMETER_TAG]);
        KeyValueMap params;
        for (JsonMap::iterator it = jsonParams.begin(); it != jsonParams.end(); ++it)
        {
            string key = it->first;
            string value = AnyCast<string>(it->second);
            params[key] = value;
        }

        ProcessorInfo pInfo(processorName, moduleName);
        pInfo.setParams(params);
        processorInfoMap[processorName] = pInfo;
    }
    return true;
}

bool QrsChainConfigurator::parseChainInfos(JsonArray &chains, 
        map<string, QrsChainInfo> &chainInfoMap) 
{
    for (size_t i = 0; i < chains.size(); i++)
    {
        JsonMap chainInfo = AnyCast<JsonMap>(chains[i]);
        string chainName = AnyCast<string>(chainInfo[CHAIN_NAME_TAG]);
        QrsChainInfo cInfo;
        cInfo.setChainName(chainName);

        JsonArray pluginPoints = AnyCast<JsonArray>(chainInfo[CHAIN_POINTS_TAG]);
        for (size_t j = 0; j < pluginPoints.size(); j++)
        {
            JsonMap point = AnyCast<JsonMap>(pluginPoints[j]);
            string pointName = AnyCast<string>(point[CHAIN_POINT_TAG]);
            JsonArray jsonProcessorNames = AnyCast<JsonArray>(point[CHAIN_PROCESSOR_TAG]);
            ProcessorNameVec processorNames;
            for (size_t k = 0; k < jsonProcessorNames.size(); k++)
            {
                processorNames.push_back(AnyCast<string>(jsonProcessorNames[k]));
            }
            cInfo.addPluginPoint(pointName, processorNames);
        }
        chainInfoMap[chainName] = cInfo;
    }
    return true;
}

END_HA3_NAMESPACE(qrs);

