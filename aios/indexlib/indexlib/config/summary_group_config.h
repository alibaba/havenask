#ifndef __INDEXLIB_SUMMARY_GROUP_CONFIG_H
#define __INDEXLIB_SUMMARY_GROUP_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/summary_config.h"

IE_NAMESPACE_BEGIN(config);
class SummaryGroupConfigImpl;
DEFINE_SHARED_PTR(SummaryGroupConfigImpl);

class SummaryGroupConfig : public autil::legacy::Jsonizable
{
public:
    SummaryGroupConfig(const std::string& groupName = "",
                       summarygroupid_t groupId = INVALID_SUMMARYGROUPID,
                       summaryfieldid_t summaryFieldIdBase = INVALID_SUMMARYFIELDID);
    ~SummaryGroupConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

public:
    const std::string& GetGroupName() const; 
    void SetGroupName(const std::string& groupName);
    summarygroupid_t GetGroupId() const; 
    bool IsDefaultGroup() const; 

    void SetCompress(bool useCompress, const std::string& compressType);
    bool IsCompress() const; 
    const std::string& GetCompressType() const; 

    bool NeedStoreSummary();
    void SetNeedStoreSummary(bool needStoreSummary);
    

    summaryfieldid_t GetSummaryFieldIdBase() const; 

    void AddSummaryConfig(const SummaryConfigPtr& summaryConfig);

    void AssertEqual(const SummaryGroupConfig& other) const;
    void AssertCompatible(const SummaryGroupConfig& other) const;

public:
    typedef std::vector<SummaryConfigPtr> SummaryConfigVec;
    typedef SummaryConfigVec::const_iterator Iterator;
    Iterator Begin() const; 
    Iterator End() const; 
    size_t GetSummaryFieldsCount() const; 

private:
    SummaryGroupConfigImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryGroupConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SUMMARY_GROUP_CONFIG_H
