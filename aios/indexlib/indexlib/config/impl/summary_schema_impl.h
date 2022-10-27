#ifndef __INDEXLIB_SUMMARY_SCHEMA_IMPL_H
#define __INDEXLIB_SUMMARY_SCHEMA_IMPL_H

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

class SummarySchemaImpl : public autil::legacy::Jsonizable
{
private:
    typedef std::vector<SummaryConfigPtr> SummaryConfigVector;

public:
    typedef SummaryConfigVector::const_iterator Iterator;

public:
    SummarySchemaImpl();
    ~SummarySchemaImpl(){};

public:
    SummaryConfigPtr GetSummaryConfig(fieldid_t fieldId) const;
    SummaryConfigPtr GetSummaryConfig(const std::string &fieldName) const;
    void AddSummaryConfig(const SummaryConfigPtr& summaryConfig,
                          summarygroupid_t groupId = DEFAULT_SUMMARYGROUPID);
    size_t GetSummaryCount() const { return mSummaryConfigs.size(); }

    Iterator Begin() const { return mSummaryConfigs.begin(); }
    Iterator End() const { return mSummaryConfigs.end(); }

    bool IsInSummary(fieldid_t fieldId) const
    {
        if (fieldId >= (fieldid_t)mFieldId2SummaryVec.size() || fieldId == INVALID_FIELDID)
        {
            return false;
        }
        return mFieldId2SummaryVec[fieldId] != SummarySchemaImpl::INVALID_SUMMARY_POSITION;
    }

    summaryfieldid_t GetSummaryFieldId(fieldid_t fieldId) const
    {
        if (!IsInSummary(fieldId))
        {
            return INVALID_SUMMARYFIELDID;
        }
        return (summaryfieldid_t)mFieldId2SummaryVec[fieldId];
    }

    fieldid_t GetMaxFieldId() const { return mMaxFieldId; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const SummarySchemaImpl& other) const;
    void AssertCompatible(const SummarySchemaImpl& other) const;

    bool NeedStoreSummary() { return mNeedStoreSummary; }
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
    void AddSummaryPosition(fieldid_t fieldId, size_t pos, summarygroupid_t groupId);

private:
    typedef std::vector<size_t> FieldId2SummaryVec;
    typedef std::vector<SummaryGroupConfigPtr> SummaryGroupConfigVec;
    typedef std::unordered_map<std::string, summarygroupid_t> SummaryGroupNameToIdMap;

private:
    SummaryConfigVector mSummaryConfigs;
    FieldId2SummaryVec mFieldId2SummaryVec;
    SummaryGroupConfigVec mSummaryGroups;
    SummaryGroupNameToIdMap mSummaryGroupIdMap;
    FieldId2SummaryVec mFieldId2GroupIdVec;
    fieldid_t mMaxFieldId;
    bool mNeedStoreSummary; //true: if all fields in attributes
    
private:
    static const size_t INVALID_SUMMARY_POSITION;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummarySchemaImpl);
IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SUMMARY_SCHEMA_IMPL_H
