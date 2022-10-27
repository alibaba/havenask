#ifndef __INDEXLIB_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_INDEX_CONFIG_IMPL_H

#include <tr1/memory>
#include <string>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/exception.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/high_freq_vocabulary_creator.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/customized_config.h"

DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(config, AdaptiveDictionaryConfig);
DECLARE_REFERENCE_CLASS(config, DictionaryConfig);
DECLARE_REFERENCE_CLASS(config, TruncateTermVocabulary);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(config);

typedef std::vector<FieldConfigPtr> FieldConfigVector;

class IndexConfigImpl;
DEFINE_SHARED_PTR(IndexConfigImpl);

class IndexConfigImpl : public autil::legacy::Jsonizable
{
public:
    IndexConfigImpl()
        : mIndexId(INVALID_INDEXID)
        , mIndexType(it_unknown)
        , mHighFreqencyTermPostingType(hp_bitmap)
        , mOptionFlag(OPTION_FLAG_ALL)
        , mVirtual(false)
        , mHasTruncate(false)
        , mIsDictInlineCompress(false)
        , mShardingType(IndexConfig::IST_NO_SHARDING)
        , mIsReferenceCompress(false)
        , mIsHashTypedDictionary(false)
        , mStatus(is_normal)
        , mOwnerOpId(INVALID_SCHEMA_OP_ID)
    {}

    IndexConfigImpl(const std::string& indexName, IndexType indexType)
        : mIndexId(INVALID_INDEXID)
        , mIndexName(indexName)
        , mIndexType(indexType)
        , mHighFreqencyTermPostingType(hp_bitmap)
        , mOptionFlag(indexType == it_expack ? EXPACK_OPTION_FLAG_ALL : OPTION_FLAG_ALL)
        , mVirtual(false)
        , mHasTruncate(false)
        , mIsDictInlineCompress(false)
        , mShardingType(IndexConfig::IST_NO_SHARDING)
        , mIsReferenceCompress(false)
        , mIsHashTypedDictionary(false)          
        , mStatus(is_normal)
        , mOwnerOpId(INVALID_SCHEMA_OP_ID)          
    {}

    IndexConfigImpl(const IndexConfigImpl& other)
        : mIndexId(other.mIndexId)
        , mIndexName(other.mIndexName)
        , mIndexType(other.mIndexType)
        , mAnalyzer(other.mAnalyzer)
        , mUseTruncateProfiles(other.mUseTruncateProfiles)
        , mHighFreqencyTermPostingType(other.mHighFreqencyTermPostingType)
        , mOptionFlag(other.mOptionFlag)
        , mVirtual(other.mVirtual)
        , mHasTruncate(other.mHasTruncate)
        , mIsDictInlineCompress(other.mIsDictInlineCompress)
        , mShardingType(other.mShardingType)
        , mIsReferenceCompress(other.mIsReferenceCompress)
        , mIsHashTypedDictionary(other.mIsHashTypedDictionary)          
        , mStatus(other.mStatus)
        , mOwnerOpId(other.mOwnerOpId)
    {}

    virtual ~IndexConfigImpl() {}

public:
    virtual uint32_t GetFieldCount() const = 0;
    virtual int32_t GetFieldIdxInPack(fieldid_t id) const { return -1; }
    virtual IndexConfig::Iterator CreateIterator() const = 0;
    virtual bool IsInIndex(fieldid_t fieldId) const = 0;
    virtual void AssertEqual(const IndexConfigImpl& other) const; //= 0;
    virtual void AssertCompatible(const IndexConfigImpl& other) const = 0;
    virtual IndexConfigImpl* Clone() const = 0;
    virtual void Check() const;
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    virtual bool CheckFieldType(FieldType ft) const = 0;
    
public:
    indexid_t GetIndexId() const { return mIndexId; }
    void SetIndexId(indexid_t id)  { mIndexId = id; }

    const std::string& GetIndexName() const { return mIndexName; }
    void SetIndexName(const std::string& indexName) { mIndexName = indexName; }

    IndexType GetIndexType() const { return mIndexType; }
    void SetIndexType(IndexType indexType) { mIndexType = indexType; }

    const std::string& GetAnalyzer() const { return mAnalyzer; }
    void SetAnalyzer(const std::string& analyzerName) { mAnalyzer = analyzerName; }

    void SetOptionFlag(optionflag_t optionFlag) { mOptionFlag = optionFlag ; }
    optionflag_t GetOptionFlag() const { return mOptionFlag; }

    void SetVirtual(bool flag) { mVirtual = flag; }
    bool IsVirtual() const { return mVirtual; }

    void Disable();
    void Delete();
    bool IsDisable() const { return mStatus == is_disable; }
    bool IsDeleted() const { return mStatus == is_deleted; }
    bool IsNormal() const { return mStatus == is_normal; }
    IndexStatus GetStatus() const { return mStatus; }

    void SetOwnerModifyOperationId(schema_opid_t opId) { mOwnerOpId = opId; }
    schema_opid_t GetOwnerModifyOperationId() const { return mOwnerOpId; }

    // TODO: reserve to be compitable with old binary.
    // remove in the next version.
    bool IsDictInlineCompress() const { return mIsDictInlineCompress; }
    void SetDictInlineCompress(bool isDictInlineCompress) 
    { mIsDictInlineCompress = isDictInlineCompress; }

    void SetIsReferenceCompress(bool isReferenceCompress)
    { mIsReferenceCompress = isReferenceCompress; }
    bool IsReferenceCompress() const { return mIsReferenceCompress; }

    void SetHashTypedDictionary(bool isHashType)
    { mIsHashTypedDictionary = isHashType; }
    bool IsHashTypedDictionary() const { return mIsHashTypedDictionary; }

    // Truncate
    void SetHasTruncateFlag(bool flag) { mHasTruncate = flag; }
    bool HasTruncate() const { return mHasTruncate; }
    void SetUseTruncateProfiles(const std::string& useTruncateProfiles)
    { mUseTruncateProfiles = useTruncateProfiles; }
    bool HasTruncateProfile(const std::string& truncateProfileName) const;
    std::vector<std::string> GetUseTruncateProfiles() const;
    void SetNonTruncateIndexName(const std::string& indexName)
    { mNonTruncIndexName = indexName; }
    const std::string& GetNonTruncateIndexName() { return mNonTruncIndexName; }
    void LoadTruncateTermVocabulary(const storage::ArchiveFolderPtr& metaFolder,
                                    const std::vector<std::string>& truncIndexNames);
    void LoadTruncateTermVocabulary(const file_system::DirectoryPtr& truncMetaDir, 
                                    const std::vector<std::string>& truncIndexNames);
    const TruncateTermVocabularyPtr& GetTruncateTermVocabulary() const
    { return mTruncateTermVocabulary; }
    bool IsTruncateTerm(dictkey_t key) const;
    bool GetTruncatePostingCount(dictkey_t key, int32_t& count) const;

    // Dict
    bool HasAdaptiveDictionary() const
    { return mAdaptiveDictConfig.get() != NULL; }
    const AdaptiveDictionaryConfigPtr& GetAdaptiveDictionaryConfig() const
    { return mAdaptiveDictConfig; }
    void SetAdaptiveDictConfig(const AdaptiveDictionaryConfigPtr& dictConfig) 
    { mAdaptiveDictConfig = dictConfig; }
    void SetDictConfig(const DictionaryConfigPtr& dictConfig) 
    {
        mDictConfig = dictConfig;
        mHightFreqVocabulary = HighFreqVocabularyCreator::CreateVocabulary(
                mIndexName, mIndexType, dictConfig);
    }
    const DictionaryConfigPtr& GetDictConfig() const { return mDictConfig; }
    void SetHighFreqencyTermPostingType(HighFrequencyTermPostingType type)
    { mHighFreqencyTermPostingType = type; }
    HighFrequencyTermPostingType GetHighFrequencyTermPostingType() const
    { return mHighFreqencyTermPostingType; }
    void SetHighFreqVocabulary(const HighFrequencyVocabularyPtr& vocabulary)
    { mHightFreqVocabulary = vocabulary; }
    const HighFrequencyVocabularyPtr& GetHighFreqVocabulary() 
    { return mHightFreqVocabulary; }
    bool IsBitmapOnlyTerm(dictkey_t key) const;    

    void SetShardingType(IndexConfig::IndexShardingType shardingType)
    {   mShardingType = shardingType; }

    IndexConfig::IndexShardingType GetShardingType() const
    {   return mShardingType; }

    void AppendShardingIndexConfig(const IndexConfigPtr& shardingIndexConfig)
    { 
        assert(mShardingType == IndexConfig::IST_NEED_SHARDING);
        mShardingIndexConfigs.push_back(shardingIndexConfig);
    }

    const std::vector<IndexConfigPtr>& GetShardingIndexConfigs() const
    { return mShardingIndexConfigs; }

    void SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs)
    { mCustomizedConfigs = customizedConfigs; }

    const CustomizedConfigVector& GetCustomizedConfigs() const
    { return mCustomizedConfigs; }

private:
    void JsonizeDictInlineFlagForLegacyBinary(
            autil::legacy::Jsonizable::JsonWrapper& json);
    
protected:
    indexid_t mIndexId;
    std::string mIndexName;
    IndexType mIndexType;

    std::string mAnalyzer;
    std::string mUseTruncateProfiles;
    std::string mNonTruncIndexName;
    AdaptiveDictionaryConfigPtr mAdaptiveDictConfig;
    DictionaryConfigPtr mDictConfig;
    HighFrequencyVocabularyPtr mHightFreqVocabulary;
    TruncateTermVocabularyPtr mTruncateTermVocabulary;
    HighFrequencyTermPostingType mHighFreqencyTermPostingType;
    optionflag_t mOptionFlag;
    bool mVirtual;
    bool mHasTruncate;
    bool mIsDictInlineCompress;
    IndexConfig::IndexShardingType mShardingType;
    bool mIsReferenceCompress;
    bool mIsHashTypedDictionary;
    std::vector<IndexConfigPtr> mShardingIndexConfigs;
    //customized config
    CustomizedConfigVector mCustomizedConfigs;

    IndexStatus mStatus;
    schema_opid_t mOwnerOpId;
    
private:
    friend class SingleFieldIndexConfigTest;
    friend class IndexConfigTest;
    IE_LOG_DECLARE();
};


IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEX_CONFIG_IMPL_H
