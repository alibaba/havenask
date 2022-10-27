#ifndef __INDEXLIB_INDEX_CONFIG_H
#define __INDEXLIB_INDEX_CONFIG_H

#include <tr1/memory>
#include <string>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/storage/archive_folder.h"

DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(config, AdaptiveDictionaryConfig);
DECLARE_REFERENCE_CLASS(config, DictionaryConfig);
DECLARE_REFERENCE_CLASS(config, TruncateTermVocabulary);
DECLARE_REFERENCE_CLASS(config, HighFrequencyVocabulary);
DECLARE_REFERENCE_CLASS(config, CustomizedConfig);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, IndexConfigImpl);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(config);

typedef std::vector<FieldConfigPtr> FieldConfigVector;
typedef std::vector<CustomizedConfigPtr> CustomizedConfigVector;

class IndexConfig : public autil::legacy::Jsonizable
{
public:
    enum IndexShardingType
    {
        IST_NO_SHARDING,
        IST_NEED_SHARDING,
        IST_IS_SHARDING,
    };

public:
    class Iterator
    {
    public:
        Iterator() {}
        Iterator(const FieldConfigVector& fieldConfigs)
            : mFieldConfigs(fieldConfigs)
            , mIdx(0)
        {}
        Iterator(const FieldConfigPtr& fieldConfig)
            : mIdx(0)
        { mFieldConfigs.push_back(fieldConfig); }

        ~Iterator() {}
    public:
        bool HasNext() const
        { return mIdx < mFieldConfigs.size(); }
        FieldConfigPtr Next()
        {
            if (mIdx < mFieldConfigs.size())
            {
                return mFieldConfigs[mIdx++];
            }
            return FieldConfigPtr();
        }
    private:
        FieldConfigVector mFieldConfigs;
        size_t mIdx;
    };

public:
    IndexConfig();
    IndexConfig(const std::string& indexName, IndexType indexType);
    IndexConfig(const IndexConfig& other);
    virtual ~IndexConfig();

public:
    virtual uint32_t GetFieldCount() const = 0;
    virtual int32_t GetFieldIdxInPack(fieldid_t id) const;
    virtual Iterator CreateIterator() const = 0;
    virtual bool IsInIndex(fieldid_t fieldId) const = 0;
    virtual void AssertEqual(const IndexConfig& other) const; //= 0;
    virtual void AssertCompatible(const IndexConfig& other) const = 0;
    virtual IndexConfig* Clone() const = 0;
    virtual void Check() const;
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    virtual bool CheckFieldType(FieldType ft) const = 0;

public:
    indexid_t GetIndexId() const;
    void SetIndexId(indexid_t id);

    const std::string& GetIndexName() const;
    void SetIndexName(const std::string& indexName);

    IndexType GetIndexType() const;
    void SetIndexType(IndexType indexType);

    const std::string& GetAnalyzer() const;
    void SetAnalyzer(const std::string& analyzerName);

    void SetOptionFlag(optionflag_t optionFlag);
    optionflag_t GetOptionFlag() const;

    void SetVirtual(bool flag);
    bool IsVirtual() const;

    void Disable();
    bool IsDisable() const;
    void Delete();
    bool IsDeleted() const;
    bool IsNormal() const;
    IndexStatus GetStatus() const;

    // TODO: reserve to be compitable with old binary.
    // remove in the next version.
    bool IsDictInlineCompress() const;
    void SetDictInlineCompress(bool isDictInlineCompress);

    void SetIsReferenceCompress(bool isReferenceCompress);
    bool IsReferenceCompress() const;

    void SetHashTypedDictionary(bool isHashType);
    bool IsHashTypedDictionary() const;

    // Truncate
    void SetHasTruncateFlag(bool flag);
    bool HasTruncate() const;
    void SetUseTruncateProfiles(const std::string& useTruncateProfiles);
    bool HasTruncateProfile(const std::string& truncateProfileName) const;
    std::vector<std::string> GetUseTruncateProfiles() const;
    void SetNonTruncateIndexName(const std::string& indexName);
    const std::string& GetNonTruncateIndexName();
    void LoadTruncateTermVocabulary(const storage::ArchiveFolderPtr& metaFolder,
                                    const std::vector<std::string>& truncIndexNames);
    void LoadTruncateTermVocabulary(const file_system::DirectoryPtr& truncMetaDir, 
                                    const std::vector<std::string>& truncIndexNames);
    const TruncateTermVocabularyPtr& GetTruncateTermVocabulary() const;
    bool IsTruncateTerm(dictkey_t key) const;
    bool GetTruncatePostingCount(dictkey_t key, int32_t& count) const;

    // Dict
    bool HasAdaptiveDictionary() const;
    const AdaptiveDictionaryConfigPtr& GetAdaptiveDictionaryConfig() const;
    void SetAdaptiveDictConfig(const AdaptiveDictionaryConfigPtr& dictConfig);
    void SetDictConfig(const DictionaryConfigPtr& dictConfig);
    const DictionaryConfigPtr& GetDictConfig() const;
    void SetHighFreqencyTermPostingType(HighFrequencyTermPostingType type);
    HighFrequencyTermPostingType GetHighFrequencyTermPostingType() const;
    void SetHighFreqVocabulary(const HighFrequencyVocabularyPtr& vocabulary);
    const HighFrequencyVocabularyPtr& GetHighFreqVocabulary();
    bool IsBitmapOnlyTerm(dictkey_t key) const;    
    void SetShardingType(IndexShardingType shardingType);
    IndexShardingType GetShardingType() const;
    void AppendShardingIndexConfig(const IndexConfigPtr& shardingIndexConfig);
    const std::vector<IndexConfigPtr>& GetShardingIndexConfigs() const;
    void SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs);
    const CustomizedConfigVector& GetCustomizedConfigs() const;

    void SetOwnerModifyOperationId(schema_opid_t opId);
    schema_opid_t GetOwnerModifyOperationId() const;

public:
    static const char* IndexTypeToStr(IndexType indexType);
    static IndexType StrToIndexType(const std::string& typeStr, TableType tableType);
    static std::string CreateTruncateIndexName(
        const std::string& indexName, const std::string& truncateProfileName);

    static std::string GetShardingIndexName(
        const std::string& indexName, size_t shardingIdx);    
    static bool GetIndexNameFromShardingIndexName(
            const std::string& shardingIndexName, std::string& indexName);
protected:
    void ResetImpl(IndexConfigImpl* impl);
    const IndexConfigImplPtr& GetImpl();

private:
    IndexConfigImplPtr mImpl;

private:
    friend class SingleFieldIndexConfigTest;
    friend class IndexConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexConfig);
typedef std::vector<IndexConfigPtr> IndexConfigVector;

class IndexConfigIterator
{
public:
    IndexConfigIterator(const IndexConfigVector& indexConfigs)
        : mConfigs(indexConfigs)
    {}

    IndexConfigVector::const_iterator Begin() const
    { return mConfigs.begin(); }
    
    IndexConfigVector::const_iterator End() const
    { return mConfigs.end(); }
    
private:
    IndexConfigVector mConfigs;
};
DEFINE_SHARED_PTR(IndexConfigIterator);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEX_CONFIG_H
