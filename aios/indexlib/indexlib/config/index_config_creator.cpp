#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/number_index_type_transformor.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/config/impl/spatial_index_config_impl.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/config/impl/customized_index_config_impl.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/config/impl/date_index_config_impl.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/impl/kv_index_config_impl.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/impl/kkv_index_config_impl.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/config/impl/range_index_config_impl.h"
#include "indexlib/config/impl/package_index_config_impl.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, IndexConfigCreator);

IndexConfigCreator::IndexConfigCreator() 
{
}

IndexConfigCreator::~IndexConfigCreator() 
{
}

string IndexConfigCreator::GetIndexName(const JsonMap& index)
{
    JsonMap::const_iterator it = index.find(INDEX_NAME);
    if (it == index.end())
    {
        INDEXLIB_FATAL_ERROR(Schema, "no index_name in indexs section.");
    }
    string indexName = AnyCast<string>(it->second);
    if (indexName.empty())
    {
        INDEXLIB_FATAL_ERROR(Schema, "index_name cannot be empty");
    }
    return indexName;
}

string IndexConfigCreator::GetAnalyzerName(const JsonMap& index)
{
    string analyzerName = "";
    JsonMap::const_iterator it = index.find(INDEX_ANALYZER);
    if (it != index.end())
    {
        analyzerName = AnyCast<string>(it->second);
    }
    return analyzerName;
}

IndexType IndexConfigCreator::GetIndexType(const JsonMap& index, TableType tableType)
{
    JsonMap::const_iterator it = index.find(INDEX_TYPE);
    if (it == index.end())
    {
        INDEXLIB_FATAL_ERROR(Schema, "no index_type in indexs section.");
    }
    string tmp = AnyCast<string>(it->second);
    return IndexConfig::StrToIndexType(tmp, tableType);
}

DictionaryConfigPtr IndexConfigCreator::GetDictConfig(
        const JsonMap& index, const DictionarySchemaPtr& dictSchema)
{
    JsonMap::const_iterator it = index.find(HIGH_FEQUENCY_DICTIONARY);
    DictionaryConfigPtr dictConfig;
    if (it != index.end())
    {
        string dictName = AnyCast<string>(it->second);
        if (dictSchema)
        {
            dictConfig = dictSchema->GetDictionaryConfig(dictName);
        }
        if (!dictConfig)
        {
            stringstream ss;
            ss << "Dictionary " << dictName << " doesn't exist";
            INDEXLIB_FATAL_ERROR(NonExist, "%s", ss.str().c_str());
        }	
    }
    return dictConfig;
}

AdaptiveDictionaryConfigPtr IndexConfigCreator::GetAdaptiveDictConfig(
        const JsonMap& index, const AdaptiveDictionarySchemaPtr& adaptiveDictSchema)
{
    JsonMap::const_iterator it = index.find(HIGH_FEQUENCY_ADAPTIVE_DICTIONARY);
    AdaptiveDictionaryConfigPtr adaptiveDictConfig;
    if (it != index.end())
    {
        string ruleName = AnyCast<string>(it->second);
        if (adaptiveDictSchema)
        {
            adaptiveDictConfig = adaptiveDictSchema->GetAdaptiveDictionaryConfig(ruleName);
        }

        if (!adaptiveDictConfig)
        {
            stringstream ss;
            ss << "Adaptive Dictionary " << ruleName << " doesn't exist";
            INDEXLIB_FATAL_ERROR(NonExist, "%s", ss.str().c_str());
        }	
    }
    return adaptiveDictConfig;
}

bool IndexConfigCreator::GetShardingConfigInfo(
        const JsonMap& index, IndexType indexType, size_t &shardingCount)
{
    JsonMap::const_iterator it = index.find(NEED_SHARDING);
    if (it == index.end())
    {
        return false;
    }

    bool needSharding = AnyCast<bool>(it->second);
    if (!needSharding)
    {
        return false;
    }

    it = index.find(SHARDING_COUNT);
    if (it == index.end())
    {
        INDEXLIB_FATAL_ERROR(Schema,
                       "sharding_count must exist when need_sharding=true");
    }

    shardingCount = JsonNumberCast<int32_t>(it->second);
    if (shardingCount <= 1)
    {
        INDEXLIB_FATAL_ERROR(Schema,
                       "sharding_count must greater than 1 when need_sharding=true");
    }

    if (indexType == it_primarykey64 || indexType == it_primarykey128 ||
        indexType == it_kv || indexType == it_kkv || indexType == it_range ||
        indexType == it_trie || indexType == it_spatial || indexType == it_customized)
    {
        INDEXLIB_FATAL_ERROR(Schema,
                       "indexType [%s] not support when need_sharding=true",
                       IndexConfig::IndexTypeToStr(indexType));
    }
    return true;
}

//TODO: index config interface refator
IndexConfigPtr IndexConfigCreator::Create(
        const FieldSchemaPtr& fieldSchema, const DictionarySchemaPtr& dictSchema,
        const AdaptiveDictionarySchemaPtr& adaptiveDictSchema, const Any& indexConfigJson,
        TableType tableType)
{
    JsonMap index = AnyCast<JsonMap>(indexConfigJson);
    string indexName = GetIndexName(index);
    string analyzerName = GetAnalyzerName(index);
    IndexType indexType = GetIndexType(index, tableType);
    IndexConfigPtr indexConfig;

    if (indexType == it_pack || indexType == it_expack
        || indexType == it_customized)
    {
        // parse field_bit_boosts
        vector<string> fieldNames;
        vector<int32_t> boosts;
        LoadPackageIndexFields(index, indexName, fieldNames, boosts);
        indexConfig = CreatePackageIndexConfig(indexName, indexType, fieldNames,
                boosts, analyzerName, false, fieldSchema);
    }
    else if (indexType == it_kkv)
    {
        return CreateKKVIndexConfig(indexName, indexType, indexConfigJson,
                fieldSchema);
    }
    else 
    {
        string fieldName;
        LoadSingleFieldIndexField(index, indexName, fieldName); 
        indexConfig = CreateSingleFieldIndexConfig(indexName, indexType,
                fieldName, analyzerName, false, fieldSchema);
    }

    CheckOptionFlag(indexType, index);
    // parse payload
    indexConfig->SetOptionFlag(CalcOptionFlag(index, indexType));

    Jsonizable::JsonWrapper jsonWrapper(indexConfigJson);
    indexConfig->Jsonize(jsonWrapper);
    if (indexType == it_number)
    {
        NumberIndexTypeTransformor::Transform(indexConfig, indexType);
        indexConfig->SetIndexType(indexType);
    }

    DictionaryConfigPtr dictConfig = GetDictConfig(index, dictSchema);
    if (dictConfig)
    {
        indexConfig->SetDictConfig(dictConfig);
    }

    AdaptiveDictionaryConfigPtr adaptiveDictConfig = GetAdaptiveDictConfig(
            index, adaptiveDictSchema);
    if (adaptiveDictConfig)
    {
        indexConfig->SetAdaptiveDictConfig(adaptiveDictConfig);
    }

    size_t shardingCount = 0;
    bool needSharding = GetShardingConfigInfo(index, indexType, shardingCount);
    if (needSharding)
    {
        CreateShardingIndexConfigs(indexConfig, shardingCount);
    }
    return indexConfig;
}

PackageIndexConfigPtr IndexConfigCreator::CreatePackageIndexConfig(
        const string& indexName, 
        IndexType indexType, 
        const vector<string>& fieldNames, 
        const vector<int32_t> boosts, 
        const string& analyzerName,
        bool isVirtual,
        const FieldSchemaPtr& fieldSchema)
{
    PackageIndexConfigPtr packageConfig;

    if (indexType == it_customized)
    {
        packageConfig.reset(new CustomizedIndexConfig(
                        indexName, indexType));
    }
    else
    {
        packageConfig.reset(new PackageIndexConfig(
                        indexName, indexType));        
    }
    packageConfig->SetFieldSchema(fieldSchema);  //important.
    packageConfig->SetVirtual(isVirtual);
    for (size_t i = 0; i < fieldNames.size(); ++i)
    {
        packageConfig->AddFieldConfig(fieldNames[i], boosts[i]);
    }

    if (!analyzerName.empty()) 
    {
        packageConfig->SetAnalyzer(analyzerName);
    }
    else
    {
        packageConfig->SetDefaultAnalyzer();
    }
    return packageConfig;
}

//TODO: refactor
SingleFieldIndexConfigPtr IndexConfigCreator::CreateSingleFieldIndexConfig(
        const string& indexName, 
        IndexType indexType, 
        const string& fieldName, 
        const string& analyzerName,
        bool isVirtual,
        const FieldSchemaPtr& fieldSchema)
{
    FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(fieldName);
    if (!fieldConfig)
    {
        stringstream ss;
        ss << "No such field defined:" << fieldName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    FieldType fieldType = fieldConfig->GetFieldType();
    if(fieldType != ft_text && !analyzerName.empty())
    {
        stringstream ss;
        ss << "index field :" << fieldName << " can not set index analyzer";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    if (fieldConfig->IsBinary())
    {
        stringstream ss;
        ss << "index field :" << fieldName << " can not set isBinary = true";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    SingleFieldIndexConfigPtr indexConfig;
    if (indexType == it_spatial)
    {
        indexConfig.reset(new SpatialIndexConfig(indexName, indexType));
    }
    else if(indexType == it_date)
    {
        indexConfig.reset(new DateIndexConfig(indexName, indexType));
    }
    else if(indexType == it_range)
    {
        indexConfig.reset(new RangeIndexConfig(indexName, indexType));
    }    
    else if (indexType == it_primarykey64 || indexType == it_primarykey128)
    {
        indexConfig.reset(new PrimaryKeyIndexConfig(indexName, indexType));
    }
    else if (indexType == it_kv)
    {
        indexConfig.reset(new KVIndexConfig(indexName, indexType));
    }
    else if (indexType == it_kkv)
    {
        assert(false);
    }
    else
    {
        indexConfig.reset(new SingleFieldIndexConfig(indexName, indexType));
    }

    indexConfig->SetFieldConfig(fieldConfig);
    indexConfig->SetVirtual(isVirtual);
    if(!analyzerName.empty())
    {
        indexConfig->SetAnalyzer(analyzerName);
    }
    return indexConfig;
}

void IndexConfigCreator::CreateShardingIndexConfigs(
        const IndexConfigPtr& indexConfig, size_t shardingCount)
{
    const string& indexName = indexConfig->GetIndexName();
    indexConfig->SetShardingType(IndexConfig::IST_NEED_SHARDING);
    for (size_t i = 0; i < shardingCount; ++i)
    {
        IndexConfigPtr shardingIndexConfig(indexConfig->Clone());
        shardingIndexConfig->SetShardingType(IndexConfig::IST_IS_SHARDING);
        shardingIndexConfig->SetIndexName(IndexConfig::GetShardingIndexName(
                        indexName, i));
        shardingIndexConfig->SetDictConfig(indexConfig->GetDictConfig());
        shardingIndexConfig->SetAdaptiveDictConfig(
                indexConfig->GetAdaptiveDictionaryConfig());
        indexConfig->AppendShardingIndexConfig(shardingIndexConfig);
    }
}

void IndexConfigCreator::LoadPackageIndexFields(const JsonMap& index,
        const string& indexName,
        vector<string>& fields, 
        vector<int32_t>& boosts)
{
    JsonMap::const_iterator indexFieldsIt = index.find(INDEX_FIELDS);
    if (indexFieldsIt == index.end()) 
    {
        stringstream ss;
        ss << "no index_fields in package_index:" << indexName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    JsonArray indexFields = AnyCast<JsonArray>(indexFieldsIt->second);
    if (indexFields.empty())
    {
        stringstream ss;
        ss << "index_fields can not be empty: package_name:" << indexName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    for (JsonArray::iterator indexFieldIt = indexFields.begin();
         indexFieldIt != indexFields.end(); ++indexFieldIt)
    {
        JsonMap indexField = AnyCast<JsonMap>(*indexFieldIt);
        JsonMap::iterator fieldElement = indexField.find(FIELD_NAME);
        if (fieldElement == indexField.end())
        {
            stringstream ss;
            ss << "no field_name found in package" << indexName;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        string fieldName = AnyCast<string>(fieldElement->second);
        fields.push_back(fieldName);
            
        fieldElement = indexField.find(FIELD_BOOST);
        if (fieldElement == indexField.end())
        {
            stringstream ss;
            ss << "no boost found for field: " << fieldName;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        boosts.push_back(JsonNumberCast<int32_t>(fieldElement->second));
    }
}

void IndexConfigCreator::LoadSingleFieldIndexField(
        const JsonMap& index, 
        const string& indexName, 
        string& fieldName)
{
    // parse index_fields of single field index
    JsonMap::const_iterator it = index.find(INDEX_FIELDS);
    if (it == index.end()) 
    {
        stringstream ss;
        ss << "no index_fields for index: " << indexName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    fieldName = AnyCast<string>(it->second);
}

void IndexConfigCreator::CheckOptionFlag(IndexType indexType,
        const JsonMap& index)
{
    string strIndexType(IndexConfig::IndexTypeToStr(indexType));
    if (indexType == it_string || indexType == it_number || 
        indexType == it_primarykey64 || indexType == it_primarykey128 ||
        indexType == it_kv || indexType == it_kkv ||
        indexType == it_trie || indexType == it_spatial || indexType == it_customized)
    {
        if (index.find(POSITION_LIST_FLAG) != index.end())
        {
            stringstream ss;
            ss << strIndexType << " index cannot configure position_list_flag.";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }

        if (index.find(POSITION_PAYLOAD_FLAG) != index.end())
        {
            stringstream ss;
            ss << strIndexType << " index cannot configure position_payload_flag.";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }            
    }

    if (indexType == it_primarykey64 || indexType == it_primarykey128 ||
        indexType == it_kv || indexType == it_kkv ||
        indexType == it_trie || indexType == it_spatial || indexType == it_customized)
    {
        if (index.find(DOC_PAYLOAD_FLAG) != index.end())
        {
            stringstream ss;
            ss << strIndexType << " index cannot configure doc_payload_flag.";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
                    
        }

        if (index.find(TERM_PAYLOAD_FLAG) != index.end())
        {
            stringstream ss;
            ss << strIndexType << " index cannot configure term_payload_flag.";
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
    }
}

void IndexConfigCreator::updateOptionFlag(const JsonMap& indexMap, 
        const string& optionType, uint8_t flag, optionflag_t& option)
{
    JsonMap::const_iterator it = indexMap.find(optionType);
    if (it != indexMap.end())
    {
        int optionFlag = JsonNumberCast<int>(it->second);
        if (optionFlag != 0)
        {
            option |= flag;
        }
        else
        {
            option &= (~flag);
        }
    }
}

//todo: move to index json
optionflag_t IndexConfigCreator::CalcOptionFlag(const JsonMap& indexMap, 
        IndexType indexType)
{
    optionflag_t optionFlag = (of_position_list | of_term_frequency);
    updateOptionFlag(indexMap, TERM_PAYLOAD_FLAG, of_term_payload, optionFlag);
    updateOptionFlag(indexMap, DOC_PAYLOAD_FLAG, of_doc_payload, optionFlag);
    updateOptionFlag(indexMap, POSITION_PAYLOAD_FLAG, of_position_payload,
                     optionFlag);
    updateOptionFlag(indexMap, POSITION_LIST_FLAG, of_position_list,
                     optionFlag);
    updateOptionFlag(indexMap, TERM_FREQUENCY_FLAG, of_term_frequency,
                     optionFlag);
    updateOptionFlag(indexMap, TERM_FREQUENCY_BITMAP, of_tf_bitmap,
                     optionFlag);

    if (indexType == it_expack)
    {
        optionFlag |= of_fieldmap;
    }

    if (((optionFlag & of_position_list) == 0) && (optionFlag & of_position_payload))
    {
        INDEXLIB_FATAL_ERROR(Schema, "position_payload_flag should not"
                           " be set while position_list_flag is not set");
    }
    
    if ((optionFlag & of_tf_bitmap)
        && (indexType != it_pack && indexType != it_expack))
    {
        // only pack & expack support term_frequency_bitmap
        optionFlag &= (~of_tf_bitmap);
    }

    if (indexType == it_primarykey128 || indexType == it_primarykey64 ||
        indexType == it_kv || indexType == it_kkv ||
        indexType == it_trie || indexType == it_spatial ||
        indexType == it_date || indexType == it_range)
    {
        optionFlag = of_none;
    }
    else if (indexType == it_string || indexType == it_number)
    {
        optionFlag &= (~of_position_list);
    }

    if ((optionFlag & of_position_list) && ((optionFlag & of_term_frequency) == 0))
    {
        INDEXLIB_FATAL_ERROR(Schema,
                           "position_list_flag should not be set while "
                           "term_frequency_flag is not set");
    }

    if ((optionFlag & of_tf_bitmap) && (!(optionFlag & of_term_frequency)))
    {
        INDEXLIB_FATAL_ERROR(Schema,"position_ft_bitmap should not be"
                           " set while term_frequency_flag is not set");
    }

    return optionFlag;
}

IndexConfigPtr IndexConfigCreator::CreateKKVIndexConfig(
        const string& indexName, IndexType indexType,
        const Any& indexConfigJson, const FieldSchemaPtr& fieldSchema)
{
    KKVIndexConfigPtr kkvIndexConfig(new KKVIndexConfig(indexName, indexType));
    Jsonizable::JsonWrapper jsonWrapper(indexConfigJson);
    kkvIndexConfig->Jsonize(jsonWrapper);
    const string &prefixFieldName = kkvIndexConfig->GetPrefixFieldName();
    FieldConfigPtr prefixFieldConfig = fieldSchema->GetFieldConfig(prefixFieldName);
    if (!prefixFieldConfig)
    {
        INDEXLIB_FATAL_ERROR(Schema, "kkv field %s not exist in field schema",
                             prefixFieldName.c_str());
    }
    kkvIndexConfig->SetPrefixFieldConfig(prefixFieldConfig);
    const string &suffixFieldName = kkvIndexConfig->GetSuffixFieldName();
    FieldConfigPtr suffixFieldConfig = fieldSchema->GetFieldConfig(suffixFieldName);
    if (!suffixFieldConfig)
    {
        INDEXLIB_FATAL_ERROR(Schema, "kkv field %s not exist in field schema",
                             suffixFieldName.c_str());
    }
    kkvIndexConfig->SetSuffixFieldConfig(suffixFieldConfig);
    return kkvIndexConfig;
}

IE_NAMESPACE_END(config);

