#ifndef __INDEXLIB_SUMMARY_SCHEMA_H
#define __INDEXLIB_SUMMARY_SCHEMA_H

#include <tr1/memory>
#include <vector>
#include <map>
#include <unordered_map>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/summary_config.h"
#include "indexlib/config/summary_group_config.h"

IE_NAMESPACE_BEGIN(config);
class SummarySchemaImpl;
DEFINE_SHARED_PTR(SummarySchemaImpl);

class SummarySchema : public autil::legacy::Jsonizable
{
private:
    typedef std::vector<SummaryConfigPtr> SummaryConfigVector;

public:
    typedef SummaryConfigVector::const_iterator Iterator;

public:
    SummarySchema();
    ~SummarySchema() {};

public:
    SummaryConfigPtr GetSummaryConfig(fieldid_t fieldId) const;
    SummaryConfigPtr GetSummaryConfig(const std::string &fieldName) const;
    void AddSummaryConfig(const SummaryConfigPtr& summaryConfig,
                          summarygroupid_t groupId = DEFAULT_SUMMARYGROUPID);
    size_t GetSummaryCount() const;

    Iterator Begin() const;
    Iterator End() const;

    bool IsInSummary(fieldid_t fieldId) const;

    summaryfieldid_t GetSummaryFieldId(fieldid_t fieldId) const;

    fieldid_t GetMaxFieldId() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const SummarySchema& other) const;
    void AssertCompatible(const SummarySchema& other) const;

    bool NeedStoreSummary();
    void SetNeedStoreSummary(bool needStoreSummary);
    void SetNeedStoreSummary(fieldid_t fieldId);

public:
    // Group
    // Refer to INVALID_SUMMARYGROUPID, DEFAULT_SUMMARYGROUPID, DEFAULT_SUMMARYGROUPNAME
    summarygroupid_t CreateSummaryGroup(const std::string& groupName);
    summarygroupid_t FieldIdToSummaryGroupId(fieldid_t fieldId) const;
    summarygroupid_t GetSummaryGroupId(const std::string& groupName) const;
    const SummaryGroupConfigPtr& GetSummaryGroupConfig(const std::string& groupName) const;
    const SummaryGroupConfigPtr& GetSummaryGroupConfig(summarygroupid_t groupId) const;
    summarygroupid_t GetSummaryGroupConfigCount() const;
    
private:
    SummarySchemaImplPtr mImpl;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummarySchema);
IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SUMMARY_SCHEMA_H
