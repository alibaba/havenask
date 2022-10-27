#ifndef ISEARCH_PROCESSORINFO_H
#define ISEARCH_PROCESSORINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

class ProcessorInfo : public autil::legacy::Jsonizable
{
public:
    ProcessorInfo();
    ProcessorInfo(std::string processorName, std::string moduleName);
    ~ProcessorInfo();
public:
    std::string getProcessorName() const;
    void setProcessorName(const std::string &processorName);

    std::string getModuleName() const;
    void setModuleName(const std::string &moduleName);

    std::string getParam(const std::string &key) const;
    void addParam(const std::string &key, const std::string &value);

    const KeyValueMap &getParams() const;
    void setParams(const KeyValueMap &params);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    bool operator==(const ProcessorInfo &other) const;

public:
    std::string _processorName;
    std::string _moduleName;
    KeyValueMap _params;
private:
    HA3_LOG_DECLARE();
};
typedef std::vector<ProcessorInfo> ProcessorInfos;

END_HA3_NAMESPACE(config);

#endif //ISEARCH_PROCESSORINFO_H
