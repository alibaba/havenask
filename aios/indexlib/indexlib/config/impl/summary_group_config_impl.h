#ifndef __INDEXLIB_SUMMARY_GROUP_CONFIG_IMPL_H
#define __INDEXLIB_SUMMARY_GROUP_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/summary_config.h"
#include "indexlib/config/summary_group_config.h"

IE_NAMESPACE_BEGIN(config);

class SummaryGroupConfigImpl : public autil::legacy::Jsonizable
{
public:
    SummaryGroupConfigImpl(const std::string& groupName = "",
                       summarygroupid_t groupId = INVALID_SUMMARYGROUPID,
                       summaryfieldid_t summaryFieldIdBase = INVALID_SUMMARYFIELDID);
    ~SummaryGroupConfigImpl();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

public:
    const std::string& GetGroupName() const { return mGroupName; }
    void SetGroupName(const std::string& groupName) { mGroupName = groupName; }
    summarygroupid_t GetGroupId() const { return mGroupId; }
    bool IsDefaultGroup() const { return mGroupId == DEFAULT_SUMMARYGROUPID; }

    void SetCompress(bool useCompress, const std::string& compressType);
    bool IsCompress() const { return mUseCompress; }
    const std::string& GetCompressType() const { return mCompressType; }

    bool NeedStoreSummary() { return mNeedStoreSummary; }
    void SetNeedStoreSummary(bool needStoreSummary)
    { mNeedStoreSummary = needStoreSummary; }

    summaryfieldid_t GetSummaryFieldIdBase() const { return mSummaryFieldIdBase; }

    void AddSummaryConfig(const SummaryConfigPtr& summaryConfig);

    void AssertEqual(const SummaryGroupConfigImpl& other) const;
    void AssertCompatible(const SummaryGroupConfigImpl& other) const;

// public:
//     typedef std::vector<fieldid_t> FieldIdVec;
//     typedef FieldIdVec::const_iterator Iterator;
//     Iterator Begin() const { return mFieldIds.begin(); }
//     Iterator End() const { return mFieldIds.end(); }    

public:
    typedef std::vector<SummaryConfigPtr> SummaryConfigVec;
    typedef SummaryConfigVec::const_iterator Iterator;
    Iterator Begin() const { return mSummaryConfigs.begin(); }
    Iterator End() const { return mSummaryConfigs.end(); }
    size_t GetSummaryFieldsCount() const { return mSummaryConfigs.size(); }

private:
    SummaryConfigVec mSummaryConfigs;
    // FieldIdVec mFieldIds;
    std::string mGroupName;
    std::string mCompressType;
    summarygroupid_t mGroupId;
    summaryfieldid_t mSummaryFieldIdBase;
    bool mUseCompress;
    bool mNeedStoreSummary;     // true: if all fields in attributes
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryGroupConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SUMMARY_GROUP_CONFIG_IMPL_H
