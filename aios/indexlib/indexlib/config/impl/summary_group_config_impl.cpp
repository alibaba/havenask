#include "indexlib/config/impl/summary_group_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"
#include "indexlib/misc/exception.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SummaryGroupConfigImpl);

SummaryGroupConfigImpl::SummaryGroupConfigImpl(const std::string& groupName,
                                               summarygroupid_t groupId,
                                               summaryfieldid_t summaryFieldIdBase)
    : mGroupName(groupName)
    , mGroupId(groupId)
    , mSummaryFieldIdBase(summaryFieldIdBase)
    , mUseCompress(false)
    , mNeedStoreSummary(false)
{
}

SummaryGroupConfigImpl::~SummaryGroupConfigImpl() 
{
}

void SummaryGroupConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON)
    {
        vector<string> summaryFields;
        for (size_t i = 0; i < mSummaryConfigs.size(); ++i)
        {
            summaryFields.push_back(mSummaryConfigs[i]->GetSummaryName());
        }
        json.Jsonize(SUMMARY_FIELDS, summaryFields);
        json.Jsonize(SUMMARY_GROUP_NAME, mGroupName);
        json.Jsonize(SUMMARY_COMPRESS, mUseCompress);
        if (mUseCompress && !mCompressType.empty())
        {
            json.Jsonize(SUMMARY_COMPRESS_TYPE, mCompressType);
        }
    }
    else
    {
        assert(false);
    }
}

void SummaryGroupConfigImpl::AddSummaryConfig(const SummaryConfigPtr& summaryConfig)
{
    mSummaryConfigs.push_back(summaryConfig);
}

void SummaryGroupConfigImpl::SetCompress(bool useCompress, const string& compressType)
{
    mUseCompress = useCompress;
    mCompressType = compressType;
}

void SummaryGroupConfigImpl::AssertEqual(const SummaryGroupConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mSummaryConfigs.size(), other.mSummaryConfigs.size(), 
                           "mSummaryConfigs's size not equal");
    for (size_t i = 0; i < mSummaryConfigs.size(); ++i)
    {
        mSummaryConfigs[i]->AssertEqual(*(other.mSummaryConfigs[i]));
    }
    IE_CONFIG_ASSERT_EQUAL(mGroupName, other.mGroupName, 
                           "mGroupName not equal");
    IE_CONFIG_ASSERT_EQUAL(mGroupId, other.mGroupId, 
                           "mGroupId not equal");
}

void SummaryGroupConfigImpl::AssertCompatible(const SummaryGroupConfigImpl& other) const
{
    IE_CONFIG_ASSERT(mSummaryConfigs.size() <= other.mSummaryConfigs.size(), 
                           "mSummaryConfigs' size not compatible");
    for (size_t i = 0; i < mSummaryConfigs.size(); ++i)
    {
        mSummaryConfigs[i]->AssertEqual(*(other.mSummaryConfigs[i]));
    }
    IE_CONFIG_ASSERT_EQUAL(mGroupName, other.mGroupName,
                           "mGroupName not equal");
    IE_CONFIG_ASSERT_EQUAL(mGroupId, other.mGroupId,
                           "mGroupId not equal");
}


IE_NAMESPACE_END(config);

