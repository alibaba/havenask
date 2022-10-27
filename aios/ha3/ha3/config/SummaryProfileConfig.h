#ifndef ISEARCH_SUMMARYPROFILECONFIG_H
#define ISEARCH_SUMMARYPROFILECONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <build_service/plugin/ModuleInfo.h>
#include <ha3/config/SummaryProfileInfo.h>
#include <ha3/config/TypeDefine.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

class SummaryProfileConfig : public autil::legacy::Jsonizable
{
public:
    SummaryProfileConfig();
    ~SummaryProfileConfig();
private:
    SummaryProfileConfig(const SummaryProfileConfig &);
    SummaryProfileConfig& operator=(const SummaryProfileConfig &);
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    const build_service::plugin::ModuleInfos& getModuleInfos() const {
        return _modules;
    }
    const SummaryProfileInfos& getSummaryProfileInfos() const {
        return _summaryProfileInfos;
    }
    const std::vector<std::string>& getRequiredAttributeFields() const {
        return _attributeFields;
    }
public:
    // for test
    void addRequiredAttributeField(const std::string &fieldName) {
        _attributeFields.push_back(fieldName);
    }
private:
    build_service::plugin::ModuleInfos _modules;
    SummaryProfileInfos _summaryProfileInfos;
    std::vector<std::string> _attributeFields;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummaryProfileConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_SUMMARYPROFILECONFIG_H
