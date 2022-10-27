#include "indexlib/config/index_config.h"
#include "indexlib/config/impl/index_config_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, IndexConfig);

IndexConfig::IndexConfig()
{}

IndexConfig::IndexConfig(const string& indexName, IndexType indexType)
{}

IndexConfig::IndexConfig(const IndexConfig& other)
    : mImpl(other.mImpl)
{}

IndexConfig::~IndexConfig() {}

int32_t IndexConfig::GetFieldIdxInPack(fieldid_t id) const
{
    return mImpl->GetFieldIdxInPack(id);
}
void IndexConfig::AssertEqual(const IndexConfig& other) const
{
    mImpl->AssertEqual(*(other.mImpl.get()));
}
void IndexConfig::Check() const
{
    mImpl->Check();
}
void IndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}

indexid_t IndexConfig::GetIndexId() const
{
    return mImpl->GetIndexId();
}
void IndexConfig::SetIndexId(indexid_t id)
{
    mImpl->SetIndexId(id);
}

const string& IndexConfig::GetIndexName() const
{
    return mImpl->GetIndexName();
}
void IndexConfig::SetIndexName(const string& indexName)
{
    mImpl->SetIndexName(indexName);
}

IndexType IndexConfig::GetIndexType() const
{
    return mImpl->GetIndexType();
}
void IndexConfig::SetIndexType(IndexType indexType)
{
    mImpl->SetIndexType(indexType);
}

const string& IndexConfig::GetAnalyzer() const
{
    return mImpl->GetAnalyzer();
}
void IndexConfig::SetAnalyzer(const string& analyzerName)
{
    mImpl->SetAnalyzer(analyzerName);
}

void IndexConfig::SetOptionFlag(optionflag_t optionFlag)
{
    mImpl->SetOptionFlag(optionFlag) ;
}
optionflag_t IndexConfig::GetOptionFlag() const
{
    return mImpl->GetOptionFlag();
}

bool IndexConfig::IsDictInlineCompress() const
{
    return mImpl->IsDictInlineCompress();
}
void IndexConfig::SetDictInlineCompress(bool isDictInlineCompress) 
{
    mImpl->SetDictInlineCompress(isDictInlineCompress);
}

void IndexConfig::SetIsReferenceCompress(bool isReferenceCompress)
{
    mImpl->SetIsReferenceCompress(isReferenceCompress);
}
bool IndexConfig::IsReferenceCompress() const
{
    return mImpl->IsReferenceCompress();
}

void IndexConfig::SetHashTypedDictionary(bool isHashType)
{
    mImpl->SetHashTypedDictionary(isHashType);
}

bool IndexConfig::IsHashTypedDictionary() const
{
    return mImpl->IsHashTypedDictionary();    
}

// Truncate
void IndexConfig::SetHasTruncateFlag(bool flag)
{
    mImpl->SetHasTruncateFlag(flag);
}
bool IndexConfig::HasTruncate() const
{
    return mImpl->HasTruncate();
}
void IndexConfig::SetUseTruncateProfiles(const string& useTruncateProfiles)
{
    mImpl->SetUseTruncateProfiles(useTruncateProfiles);
}
bool IndexConfig::HasTruncateProfile(const string& truncateProfileName) const
{
    return mImpl->HasTruncateProfile(truncateProfileName);
}
vector<string> IndexConfig::GetUseTruncateProfiles() const
{
    return mImpl->GetUseTruncateProfiles();
}
void IndexConfig::SetNonTruncateIndexName(const string& indexName)
{
    mImpl->SetNonTruncateIndexName(indexName);
}
const string& IndexConfig::GetNonTruncateIndexName()
{
    return mImpl->GetNonTruncateIndexName();
}
void IndexConfig::LoadTruncateTermVocabulary(const storage::ArchiveFolderPtr& metaFolder,
                                             const vector<string>& truncIndexNames)
{
    mImpl->LoadTruncateTermVocabulary(metaFolder, truncIndexNames);
}
void IndexConfig::LoadTruncateTermVocabulary(const file_system::DirectoryPtr& truncMetaDir, 
                                             const vector<string>& truncIndexNames)
{
    mImpl->LoadTruncateTermVocabulary(truncMetaDir, truncIndexNames);
}
const TruncateTermVocabularyPtr& IndexConfig::GetTruncateTermVocabulary() const
{
    return mImpl->GetTruncateTermVocabulary();
}
bool IndexConfig::IsTruncateTerm(dictkey_t key) const
{
    return mImpl->IsTruncateTerm(key);
}
bool IndexConfig::GetTruncatePostingCount(dictkey_t key, int32_t& count) const
{
    return mImpl->GetTruncatePostingCount(key, count);
}

// Dict
bool IndexConfig::HasAdaptiveDictionary() const
{
    return mImpl->HasAdaptiveDictionary();
}
const AdaptiveDictionaryConfigPtr& IndexConfig::GetAdaptiveDictionaryConfig() const
{
    return mImpl->GetAdaptiveDictionaryConfig();
}
void IndexConfig::SetAdaptiveDictConfig(const AdaptiveDictionaryConfigPtr& dictConfig) 
{
    mImpl->SetAdaptiveDictConfig(dictConfig);
}
void IndexConfig::SetDictConfig(const DictionaryConfigPtr& dictConfig) 
{
    mImpl->SetDictConfig(dictConfig);
}
const DictionaryConfigPtr& IndexConfig::GetDictConfig() const
{
    return mImpl->GetDictConfig();
}
void IndexConfig::SetHighFreqencyTermPostingType(HighFrequencyTermPostingType type)
{
    mImpl->SetHighFreqencyTermPostingType(type);
}
HighFrequencyTermPostingType IndexConfig::GetHighFrequencyTermPostingType() const
{
    return mImpl->GetHighFrequencyTermPostingType();
}
void IndexConfig::SetHighFreqVocabulary(const HighFrequencyVocabularyPtr& vocabulary)
{
    mImpl->SetHighFreqVocabulary(vocabulary);
}
const HighFrequencyVocabularyPtr& IndexConfig::GetHighFreqVocabulary() 
{
    return mImpl->GetHighFreqVocabulary();
}
bool IndexConfig::IsBitmapOnlyTerm(dictkey_t key) const
{
    return mImpl->IsBitmapOnlyTerm(key);
}

void IndexConfig::SetShardingType(IndexConfig::IndexShardingType shardingType)
{
    mImpl->SetShardingType(shardingType);
}

IndexConfig::IndexShardingType IndexConfig::GetShardingType() const
{
    return mImpl->GetShardingType();
}

void IndexConfig::AppendShardingIndexConfig(const IndexConfigPtr& shardingIndexConfig)
{ 
    mImpl->AppendShardingIndexConfig(shardingIndexConfig);
}

const vector<IndexConfigPtr>& IndexConfig::GetShardingIndexConfigs() const
{
    return mImpl->GetShardingIndexConfigs();
}

void IndexConfig::SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs)
{
    mImpl->SetCustomizedConfig(customizedConfigs);
}

const CustomizedConfigVector& IndexConfig::GetCustomizedConfigs() const
{
    return mImpl->GetCustomizedConfigs();
}

const char* IndexConfig::IndexTypeToStr(IndexType indexType)
{
    switch (indexType)
    {
    case it_pack: 
        return "PACK";
    case it_text:
        return "TEXT";
    case it_expack:
        return "EXPACK";
    case it_string:
        return "STRING";
    case it_enum:
        return "ENUM";
    case it_property:
        return "PROPERTY";
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
        return "NUMBER";
    case it_primarykey64:
        return "PRIMARYKEY64";
    case it_primarykey128:
        return "PRIMARYKEY128";
    case it_trie:
        return "TRIE";
    case it_spatial:
        return "SPATIAL";
    case it_customized:
        return "CUSTOMIZED"; 
    case it_kv:
    case it_kkv:
        return "PRIMARY_KEY";
    case it_date:
        return "DATE";
    case it_range:
        return "RANGE";        
    default:
        return "UNKNOWN";
    }
    return "UNKNOWN";
}
IndexType IndexConfig::StrToIndexType(const string& typeStr, TableType tableType) 
{
    if (!strcasecmp(typeStr.c_str(), "text"))
    {
        return it_text;
    }
    else if (!strcasecmp(typeStr.c_str(), "string"))
    {
        return it_string;
    }
    else if (!strcasecmp(typeStr.c_str(), "number"))
    {
        return it_number;
    }
    else if (!strcasecmp(typeStr.c_str(), "enum"))
    {
        return it_enum;
    }
    else if (!strcasecmp(typeStr.c_str(), "property"))
    {
        return it_property;
    } 
    else if (!strcasecmp(typeStr.c_str(), "pack"))
    {
        return it_pack;
    } 
    else if (!strcasecmp(typeStr.c_str(), "expack"))
    {
        return it_expack;
    } 
    else if (!strcasecmp(typeStr.c_str(), "primarykey64"))
    {
        return it_primarykey64;
    }
    else if (!strcasecmp(typeStr.c_str(), "primarykey128"))
    {
        return it_primarykey128;
    }
    else if (!strcasecmp(typeStr.c_str(), "trie"))
    {
        return it_trie;
    }
    else if (!strcasecmp(typeStr.c_str(), "spatial"))
    {
        return it_spatial;
    }
    else if (!strcasecmp(typeStr.c_str(), "date"))
    {
        return it_date;
    }
    else if (!strcasecmp(typeStr.c_str(), "range"))
    {
        return it_range;
    }    
    else if (!strcasecmp(typeStr.c_str(), "customized"))
    {
        return it_customized;
    }
    else if (!strcasecmp(typeStr.c_str(), "primary_key"))
    {
        if (tableType == tt_kv)
        {
            return it_kv;
        }
        if (tableType == tt_kkv)
        {
            return it_kkv;
        }
        INDEXLIB_FATAL_ERROR(Schema, "PRIMARY_KEY index type requires KV or KKV type table");
    }

    stringstream ss;
    ss << "Unknown index_type: " << typeStr << ", support index_type are: ";
    for (int it = 0; it < (int)it_unknown; ++it)
    {
        ss << IndexTypeToStr((IndexType)it) << ",";
    }

    INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    return it_unknown;
}

string IndexConfig::CreateTruncateIndexName(
    const string& indexName, const string& truncateProfileName)
{
    return indexName + "_" + truncateProfileName;
}

string IndexConfig::GetShardingIndexName(
    const string& indexName, size_t shardingIdx)
{
    return indexName + "_@_" + autil::StringUtil::toString<size_t>(shardingIdx);
}
    
bool IndexConfig::GetIndexNameFromShardingIndexName(
    const string& shardingIndexName, string& indexName)
{
    size_t pos = shardingIndexName.rfind("_@_");
    if (pos == string::npos)
    {
        return false;
    }
    string numStr = shardingIndexName.substr(pos + 3);
    uint64_t num = 0;
    if (!StringUtil::fromString(numStr, num))
    {
        return false;
    }
    
    indexName = shardingIndexName.substr(0, pos);
    return true;
}

void IndexConfig::ResetImpl(IndexConfigImpl* impl)
{
    mImpl.reset(impl);
}
const IndexConfigImplPtr& IndexConfig::GetImpl()
{
    return mImpl;
}
void IndexConfig::SetVirtual(bool flag)
{
    mImpl->SetVirtual(flag);
}
bool IndexConfig::IsVirtual() const
{
    return mImpl->IsVirtual();
}

void IndexConfig::Disable()
{
    mImpl->Disable();    
}

bool IndexConfig::IsDisable() const
{
    return mImpl->IsDisable();
}

void IndexConfig::Delete()
{
    mImpl->Delete();
}

bool IndexConfig::IsDeleted() const
{
    return mImpl->IsDeleted();
}

bool IndexConfig::IsNormal() const
{
    return mImpl->IsNormal();
}

IndexStatus IndexConfig::GetStatus() const
{
    return mImpl->GetStatus();
}

void IndexConfig::SetOwnerModifyOperationId(schema_opid_t opId)
{
    mImpl->SetOwnerModifyOperationId(opId);
}

schema_opid_t IndexConfig::GetOwnerModifyOperationId() const
{
    return mImpl->GetOwnerModifyOperationId();
}

IE_NAMESPACE_END(config);
