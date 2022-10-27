#include "indexlib/config/impl/package_index_config_impl.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/schema_configurator.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PackageIndexConfigImpl);

PackageIndexConfigImpl::PackageIndexConfigImpl(
        const string& indexName, IndexType indexType)
    : IndexConfigImpl(indexName, indexType)
    , mTotalFieldBoost(0)
    , mHasSectionAttribute(true)
    , mMaxFirstOccInDoc(INVALID_POSITION)
{
    mSectionAttributeConfig.reset(new SectionAttributeConfig);
}

PackageIndexConfigImpl::PackageIndexConfigImpl(const PackageIndexConfigImpl& other)
    : IndexConfigImpl(other)
    , mFieldBoosts(other.mFieldBoosts)
    , mFieldHits(other.mFieldHits)
    , mFieldIdxInPack(other.mFieldIdxInPack)
    , mIndexFields(other.mIndexFields)
    , mTotalFieldBoost(other.mTotalFieldBoost)
    , mFieldSchema(other.mFieldSchema)
    , mHasSectionAttribute(other.mHasSectionAttribute)
    , mMaxFirstOccInDoc(other.mMaxFirstOccInDoc)
    , mSectionAttributeConfig(other.mSectionAttributeConfig)
{
}

PackageIndexConfigImpl::~PackageIndexConfigImpl() 
{
}

void PackageIndexConfigImpl::AddFieldConfig(const FieldConfigPtr& fieldConfig,
                                            int32_t boost)
{
    assert(fieldConfig);
    assert(mIndexType == it_pack || mIndexType == it_expack
           || mIndexType == it_customized);

    FieldType fieldType = fieldConfig->GetFieldType();
    if (!CheckFieldType(fieldType))
    {
        stringstream ss;
        ss << "IndexType " << IndexConfig::IndexTypeToStr(mIndexType)
           << " and FieldType " << FieldConfig::FieldTypeToStr(fieldType)
           << " Mismatch.";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    uint32_t maxFieldNum = 0;
    switch (mIndexType) 
    {
    case it_pack:
        maxFieldNum = PackageIndexConfig::PACK_MAX_FIELD_NUM;
        break;
    case it_expack:
        maxFieldNum = PackageIndexConfig::EXPACK_MAX_FIELD_NUM;
        break;
    case it_customized:
        maxFieldNum = PackageIndexConfig::CUSTOMIZED_MAX_FIELD_NUM;
        break; 
    default:
        break;
    }

    if (mIndexFields.size() >= maxFieldNum)
    {
        stringstream ss;
        ss << "Only support at last " << maxFieldNum
           << " fields in package index";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    
    fieldid_t id = fieldConfig->GetFieldId();
    if (id < (fieldid_t)mFieldHits.size() && mFieldHits[id] != 0)
    {
        stringstream ss;
        ss << "duplicated field in package: " << GetIndexName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    mIndexFields.push_back(fieldConfig);
    SetFieldBoost(id, boost);

    if (id + 1 > (fieldid_t)mFieldHits.size())
    {
        mFieldHits.resize(id + 1, (firstocc_t)0);
    }
    mFieldHits[id] = mIndexFields.size();

    if (id + 1 > (fieldid_t)mFieldIdxInPack.size())
    {
        mFieldIdxInPack.resize(id + 1, (int32_t)-1);
    }
    mFieldIdxInPack[id] = mIndexFields.size() - 1;
}

void PackageIndexConfigImpl::AddFieldConfig(const std::string& fieldName, int32_t boost)
{
    assert(mFieldSchema);
    FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(fieldName);
    if (!fieldConfig)
    {
        stringstream ss;
        ss << "No such field in field definition: " << fieldName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    AddFieldConfig(fieldConfig, boost);
}

void PackageIndexConfigImpl::AddFieldConfig(fieldid_t fieldId, int32_t boost)
{
    assert(mFieldSchema);
    FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(fieldId);
    if (!fieldConfig)
    {
        stringstream msg;
        msg << "No such field in field definition:" << fieldId;
        INDEXLIB_FATAL_ERROR(Schema, "%s", msg.str().c_str());
    }
    AddFieldConfig(fieldConfig, boost);
}

void PackageIndexConfigImpl::SetFieldBoost(fieldid_t id, int32_t boost)
{
    if (id + 1 > (fieldid_t)mFieldBoosts.size())
    {
        mFieldBoosts.resize(id + 1, 0);
    }
    mTotalFieldBoost += boost - mFieldBoosts[id];
    mFieldBoosts[id] = boost;
}

fieldid_t PackageIndexConfigImpl::GetFieldId(int32_t fieldIdxInPack) const
{
    if (fieldIdxInPack >= 0 
        && fieldIdxInPack < (int32_t)mIndexFields.size()) 
    {
        return mIndexFields[fieldIdxInPack]->GetFieldId();
    }
    return INVALID_FIELDID;
}


bool PackageIndexConfigImpl::IsInIndex(fieldid_t fieldId) const
{
    if (fieldId < 0 || fieldId >= (fieldid_t)mFieldIdxInPack.size()) 
    {
        return false;
    }
    return mFieldIdxInPack[fieldId] >= 0;
}

void PackageIndexConfigImpl::AssertEqual(const IndexConfigImpl& other) const
{
    IndexConfigImpl::AssertEqual(other);
    const PackageIndexConfigImpl& other2 = (const PackageIndexConfigImpl&)other;
    IE_CONFIG_ASSERT_EQUAL(mFieldBoosts, other2.mFieldBoosts,
                           "mFieldBoosts not equal");
    IE_CONFIG_ASSERT_EQUAL(mTotalFieldBoost, other2.mTotalFieldBoost,
                           "mTotalFieldBoost not equal");
    IE_CONFIG_ASSERT_EQUAL(mIndexFields.size(), other2.mIndexFields.size(),
                           "mIndexFields's size not equal");
    IE_CONFIG_ASSERT_EQUAL(mHasSectionAttribute, other2.mHasSectionAttribute,
                           "mHasSectionAttribute not equal");
    IE_CONFIG_ASSERT_EQUAL(mMaxFirstOccInDoc, other2.mMaxFirstOccInDoc,
                           "mMaxFirstOccInDoc not equal");
    for (size_t i = 0; i < mIndexFields.size(); ++i)
    {
        mIndexFields[i]->AssertEqual(*(other2.mIndexFields[i]));
    }

    IE_CONFIG_ASSERT_EQUAL((mDictConfig != NULL), 
                           (other2.mDictConfig != NULL),
                           "high frequency dictionary config is not equal");
    if (mDictConfig)
    {
        mDictConfig->AssertEqual(*other2.mDictConfig);
    }
    IE_CONFIG_ASSERT_EQUAL(mHighFreqencyTermPostingType, 
                           other2.mHighFreqencyTermPostingType,
                           "high frequency term posting type is not equal");

    if (mSectionAttributeConfig && other2.mSectionAttributeConfig)
    {
        mSectionAttributeConfig->AssertEqual(*other2.mSectionAttributeConfig);
    }
    else if (mSectionAttributeConfig || other2.mSectionAttributeConfig)
    {
        INDEXLIB_FATAL_ERROR(Schema, "section_attribute_config not equal!");
    }
}

void PackageIndexConfigImpl::AssertCompatible(const IndexConfigImpl& other) const
{
    AssertEqual(other);
}

void PackageIndexConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (mVirtual)
    {
        return;
    }

    if (json.GetMode() == TO_JSON) 
    {
        IndexConfigImpl::Jsonize(json);

        if (!mIndexFields.empty())
        {
            vector<JsonMap> indexFields;
            for (FieldConfigVector::iterator it = mIndexFields.begin();
                 it != mIndexFields.end(); ++it)
            {
                JsonMap fieldBoostInfo;
                fieldBoostInfo[FIELD_NAME] = (*it)->GetFieldName();
                fieldBoostInfo[FIELD_BOOST] = GetFieldBoost((*it)->GetFieldId());
                indexFields.push_back(fieldBoostInfo);
            }
            json.Jsonize(INDEX_FIELDS, indexFields);
        }

        if (mHasSectionAttribute)
        {
            SectionAttributeConfig defaultSecAttrConfig;
            if (mSectionAttributeConfig && *mSectionAttributeConfig != defaultSecAttrConfig)
            {
                json.Jsonize(SECTION_ATTRIBUTE_CONFIG, mSectionAttributeConfig);
            }
        }
        else
        {
            json.Jsonize(HAS_SECTION_ATTRIBUTE, mHasSectionAttribute);
        }

        if (mMaxFirstOccInDoc != INVALID_POSITION)
        {
            json.Jsonize(MAX_FIRSTOCC_IN_DOC, mMaxFirstOccInDoc);
        }
    }
    else
    {
        IndexConfigImpl::Jsonize(json);
        json.Jsonize(HAS_SECTION_ATTRIBUTE, mHasSectionAttribute, mHasSectionAttribute);
        if (mHasSectionAttribute)
        {
            json.Jsonize(SECTION_ATTRIBUTE_CONFIG, mSectionAttributeConfig, mSectionAttributeConfig);
        }
        else
        {
            mSectionAttributeConfig.reset();
        }
        json.Jsonize(MAX_FIRSTOCC_IN_DOC, mMaxFirstOccInDoc, mMaxFirstOccInDoc);
    }
}

IndexConfigImpl* PackageIndexConfigImpl::Clone() const
{
    return new PackageIndexConfigImpl(*this);
}

void PackageIndexConfigImpl::SetDefaultAnalyzer()
{
    string analyzer;
    for (size_t i = 0; i < mIndexFields.size(); ++i)
    {
        if (analyzer.empty())
        {
            analyzer = mIndexFields[i]->GetAnalyzerName();
            continue;
        }

        if (analyzer != mIndexFields[i]->GetAnalyzerName())
        {
            stringstream ss;
            ss << "ambiguous analyzer name in pack index, index_name = " 
               << mIndexName
               << ", analyzerName1 = " << analyzer
               << ", analyzerName2 = " << mIndexFields[i]->GetAnalyzerName();
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
    }
    mAnalyzer = analyzer;
}

const SectionAttributeConfigPtr& PackageIndexConfigImpl::GetSectionAttributeConfig() const
{
    return mSectionAttributeConfig;
}

IE_NAMESPACE_END(config);

