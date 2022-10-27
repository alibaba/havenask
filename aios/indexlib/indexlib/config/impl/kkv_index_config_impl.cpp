#include "indexlib/config/impl/kkv_index_config_impl.h"
#include "indexlib/config/configurator_define.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, KKVIndexConfigImpl);

KKVIndexConfigImpl::KKVIndexConfigImpl(const string& indexName, IndexType indexType)
    : KVIndexConfigImpl(indexName, indexType)
    , mOptStoreSKey(false)
{ }

KKVIndexConfigImpl::~KKVIndexConfigImpl() { }

IndexConfig::Iterator KKVIndexConfigImpl::CreateIterator() const
{
    FieldConfigVector fieldConfigVec;
    fieldConfigVec.push_back(mPrefixFieldConfig);
    fieldConfigVec.push_back(mSuffixFieldConfig);
    return IndexConfig::Iterator(fieldConfigVec);
}

bool KKVIndexConfigImpl::IsInIndex(fieldid_t fieldId) const
{
    return (mPrefixFieldConfig && mPrefixFieldConfig->GetFieldId() == fieldId) ||
        (mSuffixFieldConfig && mSuffixFieldConfig->GetFieldId() == fieldId);
}

void KKVIndexConfigImpl::AssertEqual(const IndexConfigImpl& other) const
{
    // TODO: use code below when kkv support no ttl
    // KVIndexConfig::AssertEqual(other);
    IndexConfigImpl::AssertEqual(other);
    
    const KKVIndexConfigImpl& other2 = (const KKVIndexConfigImpl&)other;
    mPrefixFieldConfig->AssertEqual(*(other2.mPrefixFieldConfig));
    mSuffixFieldConfig->AssertEqual(*(other2.mSuffixFieldConfig));
    mPrefixInfo.AssertEqual(other2.mPrefixInfo);
    mSuffixInfo.AssertEqual(other2.mSuffixInfo);
}

void KKVIndexConfigImpl::AssertCompatible(const IndexConfigImpl& other) const
{
    AssertEqual(other);
}

IndexConfigImpl* KKVIndexConfigImpl::Clone() const
{
    return new KKVIndexConfigImpl(*this);
}

void KKVIndexConfigImpl::Check() const
{
    IndexConfigImpl::Check();
    mIndexPreference.Check();
    
    if (mPrefixInfo.keyType != kkv_prefix)
    {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key type wrong");
    }
    if (mPrefixInfo.NeedTruncate())
    {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support set count_limits");
    }
    if (mPrefixInfo.protectionThreshold != KKVIndexFieldInfo::DEFAULT_PROTECTION_THRESHOLD)
    {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support set %s",
                             KKV_BUILD_PROTECTION_THRESHOLD.c_str());
    }
    if (!mPrefixInfo.sortParams.empty())
    {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support set trunc_sort_params");
    }
    
    if (mSuffixInfo.keyType != kkv_suffix)
    {
        INDEXLIB_FATAL_ERROR(Schema, "suffix key type wrong");
    }

    if (mPrefixFieldConfig->IsMultiValue())
    {
        INDEXLIB_FATAL_ERROR(Schema, "prefix key not support multi_value field!");
    }

    if (mSuffixFieldConfig->IsMultiValue())
    {
        INDEXLIB_FATAL_ERROR(Schema, "suffix key not support multi_value field!");
    }

    if (mSuffixInfo.NeedTruncate() &&
        mSuffixInfo.countLimits > mSuffixInfo.protectionThreshold)
    {
        INDEXLIB_FATAL_ERROR(Schema, "suffix count_limits [%u] "
                             "should be less than %s [%u]",
                             mSuffixInfo.countLimits,
                             KKV_BUILD_PROTECTION_THRESHOLD.c_str(),
                             mSuffixInfo.protectionThreshold);
    }
    
    CheckSortParams();
}

void KKVIndexConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    IndexConfigImpl::Jsonize(json);
    if (json.GetMode() == TO_JSON)
    {
        vector<KKVIndexFieldInfo> fieldInfo;
        fieldInfo.push_back(mPrefixInfo);
        fieldInfo.push_back(mSuffixInfo);
        json.Jsonize(INDEX_FIELDS, fieldInfo);
        json.Jsonize(INDEX_PREFERENCE_NAME, mIndexPreference);
        json.Jsonize(USE_NUMBER_HASH, mUseNumberHash);
    }
    else
    {
        vector<KKVIndexFieldInfo> fieldInfo;
        json.Jsonize(INDEX_FIELDS, fieldInfo);
        if (fieldInfo.size() != 2)
        {
            INDEXLIB_FATAL_ERROR(Schema, "kkv index config must has two fields");
        }
        if (fieldInfo[0].keyType == kkv_prefix)
        {
            mPrefixInfo = fieldInfo[0];
            mSuffixInfo = fieldInfo[1];
        }
        else
        {
            mPrefixInfo = fieldInfo[1];
            mSuffixInfo = fieldInfo[0];
        }
        json.Jsonize(INDEX_PREFERENCE_NAME, mIndexPreference, mIndexPreference);
        InitDefaultSortParams();
        InitDefaultSkipListThreshold();
        DisableUnsupportParam();
        json.Jsonize(USE_NUMBER_HASH, mUseNumberHash, mUseNumberHash);
    }
}

void KKVIndexConfigImpl::InitDefaultSortParams()
{
    if (mSuffixInfo.sortParams.empty() &&
        (mSuffixInfo.NeedTruncate() || mSuffixInfo.enableKeepSortSequence))
    {
        SortParam param;
        param.SetSortField(KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME);
        IE_LOG(INFO, "use default sort param : [%s]", param.toSortDescription().c_str());
        mSuffixInfo.sortParams.push_back(param);
    }
}

void KKVIndexConfigImpl::InitDefaultSkipListThreshold()
{
    if (mSuffixInfo.skipListThreshold == KKVIndexFieldInfo::INVALID_SKIPLIST_THRESHOLD)
    {
        mSuffixInfo.skipListThreshold = KKVIndexConfig::DEFAULT_SKEY_SKIPLIST_THRESHOLD;
    }
}

void KKVIndexConfigImpl::DisableUnsupportParam()
{
    IE_LOG(INFO, "Disable enable_compact_hash_key & enable_shorten_offset for kkv table");

    KKVIndexPreference::HashDictParam dictParam = mIndexPreference.GetHashDictParam();
    dictParam.SetEnableCompactHashKey(false);
    dictParam.SetEnableShortenOffset(false);
    mIndexPreference.SetHashDictParam(dictParam);
}

void KKVIndexConfigImpl::CheckSortParams() const
{
    vector<AttributeConfigPtr> subAttrConfigs;
    const ValueConfigPtr& valueConfig = GetValueConfig();
    for (size_t i = 0; i < valueConfig->GetAttributeCount(); ++i)
    {
        subAttrConfigs.push_back(valueConfig->GetAttributeConfig(i));
    }
    
    for (size_t i = 0; i < mSuffixInfo.sortParams.size(); i++)
    {
        SortParam param = mSuffixInfo.sortParams[i];
        string sortField = param.GetSortField();
        if (sortField == KKVIndexConfig::DEFAULT_SKEY_TS_TRUNC_FIELD_NAME)
        {
            continue;
        }
        if (sortField == mSuffixFieldConfig->GetFieldName())
        {
            if (mSuffixFieldConfig->GetFieldType() == ft_string)
            {
                INDEXLIB_FATAL_ERROR(Schema,
                        "sort field [%s] in trunc_sort_params "
                        "must be non string type!",
                        sortField.c_str());
            }
            continue;
        }
        bool match = false;
        for (size_t j = 0; j < subAttrConfigs.size(); ++j)
        {
            if (sortField == subAttrConfigs[j]->GetAttrName())
            {
                if (subAttrConfigs[j]->GetFieldType() == ft_string ||
                    subAttrConfigs[j]->IsMultiValue())
                {
                    INDEXLIB_FATAL_ERROR(Schema,
                            "sort field [%s] in trunc_sort_params "
                            "must be non string type & single value field!",
                            sortField.c_str());
                }
                match = true;
            }
        }
        if (!match)
        {
            INDEXLIB_FATAL_ERROR(Schema,
                    "sort field [%s] in trunc_sort_params not exist!", sortField.c_str());
        }
    }
}

IE_NAMESPACE_END(config);

