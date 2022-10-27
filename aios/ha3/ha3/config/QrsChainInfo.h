#ifndef ISEARCH_QRSCHAININFO_H
#define ISEARCH_QRSCHAININFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

typedef std::vector<std::string> ProcessorNameVec;
typedef std::map<std::string, ProcessorNameVec> QrsPluginPoints;
class QrsChainInfo : public autil::legacy::Jsonizable
{
public:
    QrsChainInfo() 
        : _chainName("DEFAULT") {}
    QrsChainInfo(const std::string &chainName) 
        : _chainName(chainName) {}
    ~QrsChainInfo() {}
public:
    const std::string& getChainName() const {return _chainName;}
    void setChainName(const std::string& chainName){_chainName = chainName;}
    ProcessorNameVec getPluginPoint(const std::string& pointName) const;
    void addPluginPoint(const std::string &pointName, 
                        const ProcessorNameVec &pointProcessors);

    void addProcessor(const std::string &pointName,
                      const std::string &processorName);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

public:
    std::string _chainName;
    QrsPluginPoints _pluginPoints;

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(config);

#endif //ISEARCH_QRSCHAININFO_H
