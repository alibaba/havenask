#include <assert.h>
#include <fstream>
#include <map>
#include <stdlib.h>
#include <unistd.h>

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/testutil/unittest.h"
using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace config {

class SchemaConfiguratorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SchemaConfiguratorTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForIndexPartitionSchema()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");
        assert(mIndexPartitionSchema.get() != NULL);
        ASSERT_EQ(string("auction"), mIndexPartitionSchema->GetSchemaName());

        ASSERT_TRUE(mIndexPartitionSchema->GetDictSchema().get() != NULL);
        ASSERT_TRUE(mIndexPartitionSchema->GetFieldSchema().get() != NULL);
        ASSERT_TRUE(mIndexPartitionSchema->GetIndexSchema().get() != NULL);
        ASSERT_TRUE(mIndexPartitionSchema->GetAttributeSchema().get() != NULL);
        ASSERT_TRUE(mIndexPartitionSchema->GetSummarySchema().get() != NULL);
    }

    void TestCaseForIndexAnalyzer()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_analyzer_example.json");
        assert(mIndexPartitionSchema.get() != NULL);
        IndexSchemaPtr indexSchema = mIndexPartitionSchema->GetIndexSchema();
        ASSERT_TRUE(indexSchema.get() != NULL);
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("phrase");
        ASSERT_TRUE("hello" == indexConfig->GetAnalyzer());
        indexConfig = indexSchema->GetIndexConfig("title");
        ASSERT_TRUE("helloworld" == indexConfig->GetAnalyzer());
    }

    void TestCaseForIndexAnalyzerException()
    {
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_analyzer_exception.json"),
                     SchemaException);
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_analyzer_exception1.json"),
                     SchemaException);
    }

    void TestCaseForTableType()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");
        EXPECT_EQ(tt_index, mIndexPartitionSchema->GetTableType());
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "normal_table_type.json");
        EXPECT_EQ(tt_index, mIndexPartitionSchema->GetTableType());
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "rawtext_table_type.json");
        EXPECT_EQ(tt_linedata, mIndexPartitionSchema->GetTableType());
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "unknown_table_type.json"),
                     SchemaException);
    }

    void TestCaseForShardingIndex()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_sharding_example.json");
        assert(mIndexPartitionSchema.get() != NULL);
        IndexSchemaPtr indexSchema = mIndexPartitionSchema->GetIndexSchema();
        ASSERT_TRUE(indexSchema.get() != NULL);

        CheckShardingIndexConfig(indexSchema, "phrase", "", 4);
        // check trunc index for phrase
        CheckShardingIndexConfig(indexSchema, "phrase", "desc_user_name", 4);

        CheckShardingIndexConfig(indexSchema, "title", "", 3);
        // check trunc index for title
        CheckShardingIndexConfig(indexSchema, "title", "desc_user_name", 3);
        CheckShardingIndexConfig(indexSchema, "title", "desc_product_id", 3);

        // NEED_SHARDING : 5, IS_SHARDING : 4 + 4 + 3 + 3 + 3, NO_SHARDING : 1
        ASSERT_EQ((size_t)23, indexSchema->GetIndexCount());

        // test jsonize
        autil::legacy::Any any = autil::legacy::ToJson(*mIndexPartitionSchema);
        IndexPartitionSchema schema;
        SchemaConfigurator configurator;
        configurator.Configurate(any, *(schema.mImpl.get()), false);
        ASSERT_NO_THROW(mIndexPartitionSchema->AssertEqual(schema));
    }

    void TestCaseForShardingIndexException()
    {
        // no sharding_count
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_sharding_exception_1.json"),
                     util::SchemaException);

        // sharding_count <= 1
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_sharding_exception_2.json"),
                     util::SchemaException);

        // indexType == pk
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_sharding_exception_3.json"),
                     util::SchemaException);

        // indexType == trie
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_sharding_exception_4.json"),
                     util::SchemaException);

        // indexType == location
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_sharding_exception_5.json"),
                     util::SchemaException);
    }

    void TestCaseForDotCharInFieldNameException()
    {
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_dot_char_exception.json"),
                     util::SchemaException);
    }

    void TestCaseForDictionarySchema()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");
        DictionarySchemaPtr dictSchema = mIndexPartitionSchema->GetDictSchema();
        ASSERT_TRUE(dictSchema != NULL);

        std::shared_ptr<DictionaryConfig> dictConfig = dictSchema->GetDictionaryConfig("top10");
        ASSERT_TRUE(dictConfig != NULL);
        ASSERT_TRUE(dictConfig->GetDictName() == "top10");
        ASSERT_TRUE(dictConfig->GetContent() == "of;a;an");

        dictConfig = dictSchema->GetDictionaryConfig("top100");
        ASSERT_TRUE(dictConfig != NULL);
        ASSERT_TRUE(dictConfig->GetDictName() == "top100");
        ASSERT_TRUE(dictConfig->GetContent() == "0;1;2;a;an");
        // TODO: add test for vocabulary hash key
    }

    void TestCaseForAdaptiveDictionarySchema()
    {
        mIndexPartitionSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_schema_with_adaptive_dictionary.json");
        {
            // adaptive schema
            AdaptiveDictionarySchemaPtr adaptiveSchema = mIndexPartitionSchema->GetAdaptiveDictSchema();
            std::shared_ptr<AdaptiveDictionaryConfig> adaptiveConfig =
                adaptiveSchema->GetAdaptiveDictionaryConfig("df");
            string ruleName = adaptiveConfig->GetRuleName();
            ASSERT_EQ("df", ruleName);
            AdaptiveDictionaryConfig::DictType type = adaptiveConfig->GetDictType();
            ASSERT_EQ(0, type);
            int32_t threshold = adaptiveConfig->GetThreshold();
            ASSERT_EQ(500000, threshold);

            adaptiveConfig = adaptiveSchema->GetAdaptiveDictionaryConfig("percent");
            ruleName = adaptiveConfig->GetRuleName();
            ASSERT_EQ("percent", ruleName);
            type = adaptiveConfig->GetDictType();
            ASSERT_EQ(1, type);
            threshold = adaptiveConfig->GetThreshold();
            ASSERT_EQ(30, threshold);

            adaptiveConfig = adaptiveSchema->GetAdaptiveDictionaryConfig("size");
            ruleName = adaptiveConfig->GetRuleName();
            ASSERT_EQ("size", ruleName);
            type = adaptiveConfig->GetDictType();
            ASSERT_EQ(2, type);
        }

        {
            IndexSchemaPtr indexSchema = mIndexPartitionSchema->GetIndexSchema();
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("phrase");
            ASSERT_TRUE(indexConfig->HasAdaptiveDictionary());
            std::shared_ptr<AdaptiveDictionaryConfig> adaptiveConfig = indexConfig->GetAdaptiveDictionaryConfig();
            string ruleName = adaptiveConfig->GetRuleName();
            ASSERT_EQ("df", ruleName);
            AdaptiveDictionaryConfig::DictType type = adaptiveConfig->GetDictType();
            ASSERT_EQ(0, type);
            int32_t threshold = adaptiveConfig->GetThreshold();
            ASSERT_EQ(500000, threshold);
        }
    }
    void TestCaseForEmptyDictionarySchema()
    {
        mIndexPartitionSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example_empty_dict.json");
        DictionarySchemaPtr dictSchema = mIndexPartitionSchema->GetDictSchema();
        AdaptiveDictionarySchemaPtr adaptiveSchema = mIndexPartitionSchema->GetAdaptiveDictSchema();
        ASSERT_TRUE(dictSchema == NULL);
        ASSERT_TRUE(adaptiveSchema == NULL);
    }

    void TestCaseForSeparator()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "separator_test_schema.json");
        auto fieldSchema = mIndexPartitionSchema->GetFieldSchema();
        auto title = fieldSchema->GetFieldConfig(0);
        ASSERT_TRUE(title);
        ASSERT_EQ(INDEXLIB_MULTI_VALUE_SEPARATOR_STR, title->GetSeparator());
        auto category = fieldSchema->GetFieldConfig(1);
        ASSERT_TRUE(category);
        ASSERT_EQ("|", category->GetSeparator());
        auto productId = fieldSchema->GetFieldConfig(2);
        ASSERT_TRUE(productId);
        ASSERT_EQ(INDEXLIB_MULTI_VALUE_SEPARATOR_STR, productId->GetSeparator());
    }

    std::string CaseDescStr(const std::string& subNum, const std::string& hint, const std::string& originalStr)
    {
        return std::string(subNum + " [hint:" + hint + "(" + originalStr + ")]");
    }

    bool ParseExpectOpen(const std::string& expectResultStr, bool& expectMultiValueOpen,
                         bool& expectUpdatableMultiValueOpen)
    {
        // expectResultStr formate:
        //["true;false"]-->[multi_value:true;updatable_multi_value:false]
        autil::StringTokenizer st(expectResultStr, ";",
                                  StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (st.getNumTokens() != 2) {
            return false;
        }

        expectMultiValueOpen = (st[0].compare("true") == 0) ? true : false;
        expectUpdatableMultiValueOpen = (st[1].compare("true") == 0) ? true : false;
        return true;
    }

    FieldConfigPtr GetFieldConfig(const std::string& fieldName)
    {
        mIndexPartitionSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_updatable_multi_value_example.json");

        assert(mIndexPartitionSchema.get() != NULL);
        FieldSchemaPtr fieldSchema = mIndexPartitionSchema->GetFieldSchema();
        FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(fieldName);
        return fieldConfig;
    }

    void CheckMultiValueAndUpdatableMultiValueOpen(const std::string& subNum, const std::string& expectResultStr,
                                                   const std::string& fieldName)
    {
        bool expectMultiValueOpen = false;
        bool expectUpdatableMultiValueOpen = false;
        bool obtainRet = ParseExpectOpen(expectResultStr, expectMultiValueOpen, expectUpdatableMultiValueOpen);
        ASSERT_TRUE(obtainRet) << subNum << "expectResultStr formate error,"
                               << "please check, expectResultStr" << expectResultStr;

        FieldConfigPtr fieldConfig = GetFieldConfig(fieldName);
        auto attrConfig = mIndexPartitionSchema->GetAttributeSchema()->GetAttributeConfig(fieldName);
        if (!attrConfig) {
            attrConfig = std::make_shared<AttributeConfig>();
            attrConfig->Init(fieldConfig);
        }

        ASSERT_EQ(expectMultiValueOpen, fieldConfig->IsMultiValue())
            << subNum << "multi_value_open failed, expect multi_value_open"
            << (expectMultiValueOpen ? "true" : "false");
        ASSERT_EQ(expectUpdatableMultiValueOpen, attrConfig->IsAttributeUpdatable())
            << subNum << "updateable_multi_value_open failed,"
            << "expect updateable_multi_value_open" << (expectUpdatableMultiValueOpen ? "true" : "false");
    }

    void TestCaseForFieldSchemaUpdatableMultiValue()
    {
        //["true;false"]-->[multi_value:true;updatable_multi_value:false]

        // test: TEXT type --> default value
        ASSERT_THROW(CheckMultiValueAndUpdatableMultiValueOpen("sub:1", "false;false", "title"),
                     indexlib::util::SchemaException);

        // test: STRING type --> default value
        CheckMultiValueAndUpdatableMultiValueOpen("sub:2", "false;false", "product_id3");

        // test: LONG type --> default value
        CheckMultiValueAndUpdatableMultiValueOpen("sub:3", "false;true", "product_id");

        // test: STRING type, multi_value set ture --> default updatable_multi_value
        CheckMultiValueAndUpdatableMultiValueOpen("sub:4", "true;false", "user_name");

        // test: LONG type, multi_value set ture --> default updatable_mult_value
        CheckMultiValueAndUpdatableMultiValueOpen("sub:5", "true;false", "product_id1");

        // test: set updatable_multi_value true
        CheckMultiValueAndUpdatableMultiValueOpen("sub:6", "true;true", "user_id");

        // test: set updatable_multi_value false
        CheckMultiValueAndUpdatableMultiValueOpen("sub:7", "true;false", "price");

        // test: set updatable_multi_value false
        //                --> single_value updatable_multi_value still true
        CheckMultiValueAndUpdatableMultiValueOpen("sub:8", "false;true", "product_id2");
    }

    void TestCaseForFieldSchema()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");

        assert(mIndexPartitionSchema.get() != NULL);

        FieldSchemaPtr fieldSchema = mIndexPartitionSchema->GetFieldSchema();
        ASSERT_EQ((size_t)17, fieldSchema->GetFieldCount());

        FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig("title");
        ASSERT_TRUE(fieldConfig.get() != NULL);
        ASSERT_EQ(0, fieldConfig->GetFieldId());
        ASSERT_EQ(string("title"), fieldConfig->GetFieldName());
        ASSERT_EQ((FieldType)ft_text, fieldConfig->GetFieldType());
        ASSERT_TRUE(!fieldConfig->IsMultiValue());
        ASSERT_EQ(string("taobao_analyzer"), fieldConfig->GetAnalyzerName());

        fieldConfig = fieldSchema->GetFieldConfig("user_name");
        auto attrConfig = mIndexPartitionSchema->GetAttributeSchema()->GetAttributeConfig("user_name");
        ASSERT_TRUE(attrConfig);
        ASSERT_TRUE(fieldConfig.get() != NULL);
        ASSERT_EQ(1, fieldConfig->GetFieldId());
        ASSERT_EQ(string("user_name"), fieldConfig->GetFieldName());
        ASSERT_EQ((FieldType)ft_string, fieldConfig->GetFieldType());
        ASSERT_TRUE(!fieldConfig->IsMultiValue());
        ASSERT_EQ(string(""), fieldConfig->GetAnalyzerName());
        ASSERT_TRUE(attrConfig->IsUniqEncode());
        ASSERT_EQ(string("hello world"),
                  *GetTypeValueFromJsonMap(fieldConfig->GetUserDefinedParam(), "key", std::string()));

        fieldConfig = fieldSchema->GetFieldConfig("user_id");
        attrConfig = mIndexPartitionSchema->GetAttributeSchema()->GetAttributeConfig("user_id");
        ASSERT_TRUE(attrConfig);
        ASSERT_TRUE(fieldConfig.get() != NULL);
        ASSERT_EQ(2, fieldConfig->GetFieldId());
        ASSERT_EQ(string("user_id"), fieldConfig->GetFieldName());
        ASSERT_EQ((FieldType)ft_integer, fieldConfig->GetFieldType());
        ASSERT_TRUE(!fieldConfig->IsMultiValue());
        ASSERT_EQ(string(""), fieldConfig->GetAnalyzerName());
        ASSERT_TRUE(!attrConfig->IsUniqEncode());

        fieldConfig = fieldSchema->GetFieldConfig(3);
        ASSERT_TRUE(fieldConfig.get() != NULL);
        ASSERT_EQ(3, fieldConfig->GetFieldId());
        ASSERT_EQ(string("price"), fieldConfig->GetFieldName());
        ASSERT_EQ((FieldType)ft_integer, fieldConfig->GetFieldType());
        ASSERT_TRUE(!fieldConfig->IsMultiValue());
        ASSERT_EQ(string(""), fieldConfig->GetAnalyzerName());

        fieldConfig = fieldSchema->GetFieldConfig(4);
        attrConfig = mIndexPartitionSchema->GetAttributeSchema()->GetAttributeConfigByFieldId(4);
        ASSERT_TRUE(attrConfig);
        ASSERT_TRUE(fieldConfig.get() != NULL);
        ASSERT_EQ(4, fieldConfig->GetFieldId());
        ASSERT_EQ(string("category"), fieldConfig->GetFieldName());
        ASSERT_EQ((FieldType)ft_integer, fieldConfig->GetFieldType());
        ASSERT_TRUE(fieldConfig->IsMultiValue());
        ASSERT_TRUE(attrConfig->IsUniqEncode());

        fieldConfig = fieldSchema->GetFieldConfig(5);
        ASSERT_TRUE(fieldConfig.get() != NULL);
        ASSERT_EQ(5, fieldConfig->GetFieldId());
        ASSERT_EQ(string("auction_type"), fieldConfig->GetFieldName());
        ASSERT_EQ((FieldType)ft_enum, fieldConfig->GetFieldType());
        ASSERT_TRUE(!fieldConfig->IsMultiValue());
        ASSERT_EQ(string(""), fieldConfig->GetAnalyzerName());

        EnumFieldConfigPtr enumFieldConfig = dynamic_pointer_cast<EnumFieldConfig>(fieldConfig);
        ASSERT_TRUE(enumFieldConfig.get() != NULL);

        ASSERT_TRUE(enumFieldConfig->GetValidValues().size() == 2);
        ASSERT_EQ(string("sale"), enumFieldConfig->GetValidValues()[0]);
        ASSERT_EQ(string("buy"), enumFieldConfig->GetValidValues()[1]);

        fieldConfig = fieldSchema->GetFieldConfig(6);
        attrConfig = mIndexPartitionSchema->GetAttributeSchema()->GetAttributeConfig(6);
        ASSERT_TRUE(attrConfig);
        ASSERT_TRUE(fieldConfig.get() != NULL);
        ASSERT_EQ(6, fieldConfig->GetFieldId());
        ASSERT_EQ(string("product_id"), fieldConfig->GetFieldName());
        ASSERT_EQ((FieldType)ft_long, fieldConfig->GetFieldType());
        ASSERT_TRUE(!fieldConfig->IsMultiValue());
        ASSERT_EQ(string(""), fieldConfig->GetAnalyzerName());
        ASSERT_TRUE(!attrConfig->IsUniqEncode());

        fieldConfig = fieldSchema->GetFieldConfig("body");
        ASSERT_TRUE(fieldConfig.get() != NULL);
        ASSERT_EQ(7, fieldConfig->GetFieldId());
        ASSERT_EQ(string("body"), fieldConfig->GetFieldName());
        ASSERT_EQ((FieldType)ft_text, fieldConfig->GetFieldType());
        ASSERT_TRUE(!fieldConfig->IsMultiValue());
        ASSERT_EQ(string("taobao_analyzer"), fieldConfig->GetAnalyzerName());

        fieldConfig = fieldSchema->GetFieldConfig("b2b_body");
        ASSERT_TRUE(fieldConfig.get() != NULL);
        ASSERT_EQ(8, fieldConfig->GetFieldId());
        ASSERT_EQ(string("b2b_body"), fieldConfig->GetFieldName());
        ASSERT_EQ((FieldType)ft_text, fieldConfig->GetFieldType());
        ASSERT_TRUE(!fieldConfig->IsMultiValue());
        ASSERT_EQ(string("b2b_analyzer"), fieldConfig->GetAnalyzerName());

        // fields not in fieldSchema
        fieldConfig = fieldSchema->GetFieldConfig(-1);
        ASSERT_TRUE(fieldConfig.get() == NULL);

        fieldConfig = fieldSchema->GetFieldConfig(17);
        ASSERT_TRUE(fieldConfig.get() == NULL);

        fieldConfig = fieldSchema->GetFieldConfig("wupingkao");
        ASSERT_TRUE(fieldConfig.get() == NULL);

        fieldConfig = fieldSchema->GetFieldConfig(" title");
        ASSERT_TRUE(fieldConfig.get() == NULL);

        fieldConfig = fieldSchema->GetFieldConfig("title ");
        ASSERT_TRUE(fieldConfig.get() == NULL);
    }

    void TestCaseForIndexSchema()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");

        assert(mIndexPartitionSchema.get() != NULL);

        IndexSchemaPtr indexSchema = mIndexPartitionSchema->GetIndexSchema();
        ASSERT_EQ((size_t)7, indexSchema->GetIndexCount());
        ASSERT_TRUE(indexSchema->HasPrimaryKeyIndex());
        ASSERT_TRUE(indexSchema->HasPrimaryKeyAttribute());
        ASSERT_EQ(it_primarykey64, indexSchema->GetPrimaryKeyIndexType());
        ASSERT_EQ("product_id", indexSchema->GetPrimaryKeyIndexFieldName());
        ASSERT_EQ(6, indexSchema->GetPrimaryKeyIndexFieldId());
        PrimaryKeyIndexConfigPtr pkIndexConfig =
            std::dynamic_pointer_cast<PrimaryKeyIndexConfig>(indexSchema->GetPrimaryKeyIndexConfig());
        ASSERT_EQ(index::pk_number_hash, pkIndexConfig->GetPrimaryKeyHashType());

        // check text index
        {
            IndexConfigPtr textIndexConfig = indexSchema->GetIndexConfig("title");
            ASSERT_TRUE(textIndexConfig.get() != NULL);
            std::shared_ptr<DictionaryConfig> dictConfig = textIndexConfig->GetDictConfig();
            ASSERT_TRUE(dictConfig.get() != NULL);
            ASSERT_EQ("top10", dictConfig->GetDictName());
            ASSERT_EQ("of;a;an", dictConfig->GetContent());
            ASSERT_EQ(indexlib::index::hp_both, textIndexConfig->GetHighFrequencyTermPostingType());
        }

        // check string index
        {
            IndexConfigPtr stringIndexConfig = indexSchema->GetIndexConfig("user_name");
            ASSERT_TRUE(stringIndexConfig.get() != NULL);
            std::shared_ptr<DictionaryConfig> dictConfig = stringIndexConfig->GetDictConfig();
            ASSERT_TRUE(!dictConfig);
            ASSERT_EQ(indexlib::index::hp_bitmap, stringIndexConfig->GetHighFrequencyTermPostingType());
        }

        // for number index bitmap
        IndexConfigPtr numIndexConfig = indexSchema->GetIndexConfig("categoryp");
        ASSERT_TRUE(numIndexConfig.get() != NULL);
        ASSERT_EQ(numIndexConfig->GetHighFrequencyTermPostingType(), indexlib::index::hp_both);
        std::shared_ptr<DictionaryConfig> numDictConfig = numIndexConfig->GetDictConfig();
        ASSERT_TRUE(numDictConfig != NULL);
        ASSERT_EQ(numDictConfig->GetDictName(), "topNum");
        ASSERT_EQ(numDictConfig->GetContent(), "0;1;2;9;11");

        numIndexConfig = indexSchema->GetIndexConfig("catmap");
        ASSERT_TRUE(numIndexConfig.get() != NULL);
        ASSERT_EQ(numIndexConfig->GetHighFrequencyTermPostingType(), indexlib::index::hp_bitmap);
        numDictConfig = numIndexConfig->GetDictConfig();
        ASSERT_TRUE(numDictConfig != NULL);
        ASSERT_EQ(numDictConfig->GetDictName(), "topNum");
        ASSERT_EQ(numDictConfig->GetContent(), "0;1;2;9;11");

        // for package index
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("phrase");
        ASSERT_TRUE(indexConfig.get() != NULL);
        PackageIndexConfigPtr packageIndexConfig = dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
        ASSERT_TRUE(packageIndexConfig.get() != NULL);

        std::shared_ptr<DictionaryConfig> dictConfig = indexConfig->GetDictConfig();
        ASSERT_TRUE(dictConfig != NULL);
        ASSERT_EQ(dictConfig->GetDictName(), "top10");
        ASSERT_EQ(dictConfig->GetContent(), "of;a;an");

        ASSERT_EQ((indexid_t)0, packageIndexConfig->GetIndexId());
        ASSERT_EQ(string("phrase"), packageIndexConfig->GetIndexName());
        ASSERT_EQ((InvertedIndexType)it_pack, packageIndexConfig->GetInvertedIndexType());

        ASSERT_EQ((size_t)2, packageIndexConfig->GetFieldCount());
        ASSERT_EQ(string("title"), packageIndexConfig->GetFieldConfigVector()[0]->GetFieldName());
        ASSERT_EQ(string("body"), packageIndexConfig->GetFieldConfigVector()[1]->GetFieldName());

        // get all boost.
        ASSERT_EQ(1010, packageIndexConfig->GetTotalFieldBoost());
        ASSERT_EQ(1000, packageIndexConfig->GetFieldBoost(0));
        ASSERT_EQ(10, packageIndexConfig->GetFieldBoost(7));
        ASSERT_EQ(0, packageIndexConfig->GetFieldBoost(100000));
        ASSERT_EQ(true, packageIndexConfig->HasSectionAttribute());

        // for single field index
        const int size = 3;
        const char* field_names[] = {"title", "user_name", "product_id"};

        InvertedIndexType index_types[] = {it_text, it_string, it_primarykey64};

        for (int i = 0; i < size; ++i) {
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(field_names[i]);
            ASSERT_TRUE(indexConfig.get() != NULL);

            SingleFieldIndexConfigPtr singleFieldIndexConfig =
                dynamic_pointer_cast<SingleFieldIndexConfig>(indexConfig);
            ASSERT_TRUE(singleFieldIndexConfig.get() != NULL);

            ASSERT_EQ(string(field_names[i]), singleFieldIndexConfig->GetIndexName());
            ASSERT_EQ((InvertedIndexType)index_types[i], singleFieldIndexConfig->GetInvertedIndexType());
            ASSERT_EQ((indexid_t)(i + 1), singleFieldIndexConfig->GetIndexId());
            ASSERT_EQ((size_t)1, singleFieldIndexConfig->GetFieldCount());
            ASSERT_EQ(string(field_names[i]), singleFieldIndexConfig->GetFieldConfig()->GetFieldName());

            if (index_types[i] == it_primarykey64) {
                ASSERT_TRUE(singleFieldIndexConfig->HasPrimaryKeyAttribute());
            } else {
                ASSERT_TRUE(!singleFieldIndexConfig->HasPrimaryKeyAttribute());
            }

            fieldid_t myfid = singleFieldIndexConfig->GetFieldConfig()->GetFieldId();
            for (fieldid_t fid = 0; fid < 6; ++fid) {
                if (fid == myfid) {
                    ASSERT_TRUE(indexConfig->IsInIndex(fid));
                } else {
                    ASSERT_TRUE(!indexConfig->IsInIndex(fid));
                }
            }
        }

        indexConfig = indexSchema->GetIndexConfig("phrase2");
        ASSERT_TRUE(indexConfig.get() != NULL);
        packageIndexConfig = dynamic_pointer_cast<PackageIndexConfig>(indexConfig);
        ASSERT_TRUE(packageIndexConfig.get() != NULL);
        ASSERT_EQ((indexid_t)4, packageIndexConfig->GetIndexId());
        ASSERT_EQ(string("phrase2"), packageIndexConfig->GetIndexName());
        ASSERT_EQ((InvertedIndexType)it_expack, packageIndexConfig->GetInvertedIndexType());

        ASSERT_EQ((size_t)2, packageIndexConfig->GetFieldCount());
        ASSERT_EQ(string("title"), packageIndexConfig->GetFieldConfigVector()[0]->GetFieldName());
        ASSERT_EQ(string("body"), packageIndexConfig->GetFieldConfigVector()[1]->GetFieldName());
        // get all boost.
        ASSERT_EQ(1010, packageIndexConfig->GetTotalFieldBoost());
        ASSERT_EQ(1000, packageIndexConfig->GetFieldBoost(0));
        ASSERT_EQ(10, packageIndexConfig->GetFieldBoost(7));
        ASSERT_EQ(0, packageIndexConfig->GetFieldBoost(100000));
        ASSERT_EQ(false, packageIndexConfig->HasSectionAttribute());

        // get all fieldIdxInPack
        ASSERT_EQ((int32_t)0, packageIndexConfig->GetFieldIdxInPack(0));
        ASSERT_EQ((int32_t)1, packageIndexConfig->GetFieldIdxInPack(7));
        ASSERT_EQ((int32_t)-1, packageIndexConfig->GetFieldIdxInPack(2));
        ASSERT_EQ((int32_t)-1, packageIndexConfig->GetFieldIdxInPack(1));
        ASSERT_EQ((int32_t)-1, packageIndexConfig->GetFieldIdxInPack(3));
        ASSERT_EQ((int32_t)-1, packageIndexConfig->GetFieldIdxInPack(5));
        ASSERT_EQ((int32_t)-1, packageIndexConfig->GetFieldIdxInPack(100000));

        // get all fieldId
        ASSERT_EQ((fieldid_t)0, packageIndexConfig->GetFieldId(0));
        ASSERT_EQ((fieldid_t)7, packageIndexConfig->GetFieldId(1));
        ASSERT_EQ(INVALID_FIELDID, packageIndexConfig->GetFieldId(2));
        ASSERT_EQ(INVALID_FIELDID, packageIndexConfig->GetFieldId(-1));
        ASSERT_EQ(INVALID_FIELDID, packageIndexConfig->GetFieldId(-2));
        ASSERT_EQ(INVALID_FIELDID, packageIndexConfig->GetFieldId(5));
        ASSERT_EQ(INVALID_FIELDID, packageIndexConfig->GetFieldId(100000));

        // check isInIndex
        for (fieldid_t fid = 0; fid < 8; ++fid) {
            if (fid == 0 || fid == 7) {
                ASSERT_TRUE(indexConfig->IsInIndex(fid));
            } else {
                ASSERT_TRUE(!indexConfig->IsInIndex(fid));
            }
        }

        // for not exist index
        indexConfig = indexSchema->GetIndexConfig(" phrase");
        ASSERT_TRUE(indexConfig.get() == NULL);

        indexConfig = indexSchema->GetIndexConfig("phrase ");
        ASSERT_TRUE(indexConfig.get() == NULL);

        indexConfig = indexSchema->GetIndexConfig("wokao");
        ASSERT_TRUE(indexConfig.get() == NULL);

        indexConfig = indexSchema->GetIndexConfig((indexid_t)-1);
        ASSERT_TRUE(indexConfig.get() == NULL);

        indexConfig = indexSchema->GetIndexConfig(7);
        ASSERT_TRUE(indexConfig.get() == NULL);
    }

    void TestCaseForIndexSchemaPostingTypeWithoutDictInPack()
    {
        ASSERT_THROW(
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example_dict_in_error_index.json"),
            SchemaException);
    }

    void TestCaseForAttributeSchemeErrorSortTag()
    {
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_error_multi_sort_tag.json"),
                     SchemaException);

        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_error_sort_tag.json"),
                     SchemaException);

        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_error_attribute_field.json"),
                     SchemaException);
    }

    void TestCaseForAttributeSchema()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");

        assert(mIndexPartitionSchema.get() != NULL);

        AttributeSchemaPtr attributeSchema = mIndexPartitionSchema->GetAttributeSchema();

        ASSERT_EQ((size_t)8, attributeSchema->GetAttributeCount());

        AttributeConfigPtr attributeConfig = attributeSchema->GetAttributeConfig(0);
        ASSERT_TRUE(attributeConfig.get() != NULL);
        ASSERT_FALSE(attributeConfig->IsUniqEncode());
        ASSERT_EQ((attrid_t)0, attributeConfig->GetAttrId());
        ASSERT_EQ(string("user_id"), attributeConfig->GetAttrName());
        ASSERT_EQ((FieldType)ft_integer, attributeConfig->GetFieldType());
        ASSERT_EQ(mIndexPartitionSchema->GetFieldConfig("user_id"), attributeConfig->GetFieldConfig());

        attributeConfig = attributeSchema->GetAttributeConfig("product_id");
        ASSERT_TRUE(attributeConfig.get() != NULL);
        ASSERT_FALSE(attributeConfig->IsUniqEncode());
        ASSERT_EQ((attrid_t)1, attributeConfig->GetAttrId());
        ASSERT_EQ(string("product_id"), attributeConfig->GetAttrName());
        ASSERT_EQ((FieldType)ft_long, attributeConfig->GetFieldType());
        ASSERT_EQ(mIndexPartitionSchema->GetFieldConfig("product_id"), attributeConfig->GetFieldConfig());

        attributeConfig = attributeSchema->GetAttributeConfig("user_name");
        ASSERT_TRUE(attributeConfig.get() != NULL);
        ASSERT_TRUE(attributeConfig->IsUniqEncode());

        attributeConfig = attributeSchema->GetAttributeConfig("category");
        ASSERT_TRUE(attributeConfig.get() != NULL);
        ASSERT_TRUE(attributeConfig->IsUniqEncode());

        attributeConfig = attributeSchema->GetAttributeConfig("price2");
        ASSERT_TRUE(attributeConfig.get() != NULL);

        attributeConfig = attributeSchema->GetAttributeConfig(5); // price 3
        ASSERT_TRUE(attributeConfig.get() != NULL);

        // no such attributes
        attributeConfig = attributeSchema->GetAttributeConfig("wokao");
        ASSERT_TRUE(attributeConfig.get() == NULL);

        attributeConfig = attributeSchema->GetAttributeConfig((attrid_t)-1);
        ASSERT_TRUE(attributeConfig.get() == NULL);

        attributeConfig = attributeSchema->GetAttributeConfig(8);
        ASSERT_TRUE(attributeConfig.get() == NULL);

        attributeConfig = attributeSchema->GetAttributeConfig(" user_id");
        ASSERT_TRUE(attributeConfig.get() == NULL);

        attributeConfig = attributeSchema->GetAttributeConfig("user_id ");
        ASSERT_TRUE(attributeConfig.get() == NULL);
    }

    void TestCaseForSummarySchema()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");

        assert(mIndexPartitionSchema.get() != NULL);

        SummarySchemaPtr summarySchema = mIndexPartitionSchema->GetSummarySchema();
        ASSERT_EQ((size_t)5, summarySchema->GetSummaryCount());

        const int size = 5;
        const char* field_names[] = {"title", "user_name", "user_id", "price", "auction_type"};
        fieldid_t field_ids[] = {0, 1, 2, 3, 5};

        for (int i = 0; i < size; ++i) {
            std::shared_ptr<SummaryConfig> summaryConfig = summarySchema->GetSummaryConfig(field_ids[i]);
            ASSERT_TRUE(summaryConfig.get() != NULL);
            ASSERT_EQ(string(field_names[i]), summaryConfig->GetSummaryName());
        }
    }

    void TestCaseForFieldTypeToStr()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");

        // get from index_engine.h
        std::string confFile = "../../index_engine.h";
        ifstream in(confFile.c_str());
        if (!in) {
            return;
        }
        string line;
        bool started = false;
        string fieldStart = "enum FieldType {";
        map<int, string> fieldType2Str;
        int fieldTypeCount = 0;
        while (getline(in, line)) {
            if (line.substr(0, fieldStart.size()) == fieldStart) {
                started = true;
                continue;
            } else if (line.substr(0, 2) == string("};")) {
                started = false;
                continue;
            }

            if (started) {
                autil::StringUtil::trim(line);
                size_t pos = line.find("ft_");
                if (pos != string::npos) {
                    line[pos] = ' ';
                    line[pos + 1] = ' ';
                    line[pos + 2] = ' ';
                }

                string fieldTypeName = "";
                for (size_t i = 0; i < line.size(); ++i) {
                    if (line[i] >= 'a' && line[i] <= 'z')
                        fieldTypeName.push_back(line[i] - 'a' + 'A');
                    else if (line[i] == '_')
                        fieldTypeName.push_back(line[i]);
                }
                // std::cout << "found " << fieldTypeName << std::endl;
                if (!fieldTypeName.empty()) {
                    fieldType2Str[fieldTypeCount++] = fieldTypeName;
                }
            }
        }
        in.close();

        // start to check
        for (map<int, string>::iterator it = fieldType2Str.begin(); it != fieldType2Str.end(); ++it) {
            ASSERT_EQ(it->second, FieldConfig::FieldTypeToStr((FieldType)it->first));
        }
    }

    void TestCaseForStrToFieldType()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");

        // get from index_engine.h
        std::string confFile = "../../index_engine.h";
        ifstream in(confFile.c_str());
        if (!in) {
            return;
        }
        string line;
        bool started = false;
        string fieldStart = "enum FieldType {";
        map<string, int> fieldType2Str;
        int fieldTypeCount = 0;
        while (getline(in, line)) {
            if (line.substr(0, fieldStart.size()) == fieldStart) {
                started = true;
                continue;
            } else if (line.substr(0, 2) == string("};")) {
                started = false;
                continue;
            }

            if (started) {
                autil::StringUtil::trim(line);
                size_t pos = line.find("ft_");
                if (pos != string::npos) {
                    line[pos] = ' ';
                    line[pos + 1] = ' ';
                    line[pos + 2] = ' ';
                }

                string fieldTypeName = "";
                for (size_t i = 0; i < line.size(); ++i) {
                    if (line[i] >= 'a' && line[i] <= 'z')
                        fieldTypeName.push_back(line[i] - 'a' + 'A');
                    else if (line[i] == '_')
                        fieldTypeName.push_back(line[i]);
                }
                // std::cout << "found " << fieldTypeName << std::endl;
                if (!fieldTypeName.empty() && fieldTypeName != "UNKNOWN") {
                    // fieldType2Str[fieldTypeCount++] = fieldTypeName;
                    fieldType2Str[fieldTypeName] = fieldTypeCount++;
                }
            }
        }
        in.close();

        // start to check
        for (map<string, int>::iterator it = fieldType2Str.begin(); it != fieldType2Str.end(); ++it) {
            ASSERT_EQ(it->second, FieldConfig::StrToFieldType(it->first));
        }
    }

    void TestCaseForInvertedIndexTypeToStr()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");

        // get from index_engine.h
        std::string confFile = "../../index_engine.h";
        ifstream in(confFile.c_str());
        if (!in) {
            return;
        }
        string line;
        bool started = false;
        string indexStart = "enum InvertedIndexType {";
        map<int, string> indexType2Str;
        int indexTypeCount = 0;
        while (getline(in, line)) {
            if (line.substr(0, indexStart.size()) == indexStart) {
                started = true;
                continue;
            } else if (line.substr(0, 2) == string("};")) {
                started = false;
                continue;
            }

            if (started) {
                autil::StringUtil::trim(line);
                size_t pos = line.find("it_");
                if (pos != string::npos) {
                    line[pos] = ' ';
                    line[pos + 1] = ' ';
                    line[pos + 2] = ' ';
                }

                string indexTypeName = "";
                for (size_t i = 0; i < line.size(); ++i) {
                    if (line[i] >= 'a' && line[i] <= 'z')
                        indexTypeName.push_back(line[i] - 'a' + 'A');
                    else if (line[i] >= '0' && line[i] <= '9')
                        indexTypeName.push_back(line[i]);
                    else if (line[i] == '_')
                        indexTypeName.push_back(line[i]);
                }
                // std::cout << "found " << indexTypeName << std::endl;
                if (!indexTypeName.empty()) {
                    indexType2Str[indexTypeCount++] = indexTypeName;
                }
            }
        }
        in.close();

        // start to check
        for (map<int, string>::iterator it = indexType2Str.begin(); it != indexType2Str.end(); ++it) {
            ASSERT_EQ(it->second, IndexConfig::InvertedIndexTypeToStr((InvertedIndexType)it->first));
        }
    }

    void TestCaseForStrToIndexType()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");

        // get from index_engine.h
        std::string confFile = "../../index_engine.h";
        ifstream in(confFile.c_str());
        if (!in) {
            return;
        }
        string line;
        bool started = false;
        string indexStart = "enum InvertedIndexType {";
        map<string, int> indexType2Str;
        int indexTypeCount = 0;
        while (getline(in, line)) {
            if (line.substr(0, indexStart.size()) == indexStart) {
                started = true;
                continue;
            } else if (line.substr(0, 2) == string("};")) {
                started = false;
                continue;
            }

            if (started) {
                autil::StringUtil::trim(line);
                size_t pos = line.find("it_");
                if (pos != string::npos) {
                    line[pos] = ' ';
                    line[pos + 1] = ' ';
                    line[pos + 2] = ' ';
                }

                string indexTypeName = "";
                for (size_t i = 0; i < line.size(); ++i) {
                    if (line[i] >= 'a' && line[i] <= 'z')
                        indexTypeName.push_back(line[i] - 'a' + 'A');
                    else if (line[i] >= '0' && line[i] <= '9')
                        indexTypeName.push_back(line[i]);
                    else if (line[i] == '_')
                        indexTypeName.push_back(line[i]);
                }
                // std::cout << "found " << indexTypeName << std::endl;
                if (!indexTypeName.empty() && indexTypeName != "UNKNOWN") {
                    // indexType2Str[indexTypeCount++] = indexTypeName;
                    indexType2Str[indexTypeName] = indexTypeCount++;
                }
            }
        }
        in.close();

        // start to check
        for (map<string, int>::iterator it = indexType2Str.begin(); it != indexType2Str.end(); ++it) {
            ASSERT_EQ(it->second, IndexConfig::StrToIndexType(it->first, tt_index));
        }
    }

    void TestCaseForLoadSchemaFailForStringPKIndexWithOptionFlag()
    {
        InternalTestLoadSchemaFailForStringPKIndexWithOptionFlag("STRING");
        InternalTestLoadSchemaFailForStringPKIndexWithOptionFlag("PRIMARYKEY64");
    }

    void TestCaseForCheckOptionFlagForNumberIndex()
    {
        // check for invalid
        bool hasException = false;
        try {
            mIndexPartitionSchema =
                SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "number_schema_with_error.json");
        } catch (const util::SchemaException& e) {
            hasException = true;
        }
        ASSERT_TRUE(hasException);

        // check normal
        hasException = false;
        try {
            mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "number_schema.json");
        } catch (const util::SchemaException& e) {
            hasException = true;
        }
        ASSERT_TRUE(!hasException);
    }

    void TestCaseForCheckOptionFlagForPKIndex()
    {
        bool hasException = false;
        try {
            mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "pk_schema.json");
        } catch (const util::SchemaException& e) {
            hasException = true;
        }
        ASSERT_TRUE(!hasException);
    }

    string GenerateFlagStr(int optionFlag)
    {
        string flagStr;
        if (optionFlag & of_term_payload) {
            flagStr.append("\"term_payload_flag\" : 1\n");
        } else if (optionFlag & of_doc_payload) {
            flagStr.append("\"doc_payload_flag\" : 1\n");
        } else if (optionFlag & of_position_list) {
            flagStr.append("\"position_list_flag\" : 1\n");
        } else if (optionFlag & of_position_payload) {
            flagStr.append("\"position_payload_flag\" : 1\n");
        }

        return flagStr;
    }

    void InternalTestLoadSchemaFailForStringPKIndexWithOptionFlag(const string& strIndexType)
    {
        string schemaFile = GET_TEMP_DATA_PATH() + "/tmpSchema.json";
        for (int i = 0; i < 4; i++) {
            if (FslibWrapper::IsExist(schemaFile).GetOrThrow()) {
                FslibWrapper::DeleteFileE(schemaFile, DeleteOption::NoFence(false));
            }

            IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
            IndexPartitionSchemaMaker::MakeSchema(schema,
                                                  // Field schema
                                                  "text1:text;string1:string;string2:string;",
                                                  // Index schema
                                                  "string2:string:string2;"
                                                  // Primary key index schema
                                                  "pk:primarykey64:string1",
                                                  // Attribute schema
                                                  "string1;string2",
                                                  // Summary schema
                                                  "string1;text1");

            autil::legacy::Any any = autil::legacy::ToJson(*schema);
            string strSchema = autil::legacy::json::ToString(any);
            size_t pos = strSchema.find("indexs");
            pos = strSchema.find(strIndexType, pos);
            if (pos == string::npos) {
                ASSERT_TRUE(false);
            }

            if (strIndexType == "PRIMARYKEY64" || i > 1) {
                strSchema.insert(pos + strIndexType.size() + 1, "," + GenerateFlagStr(1 << i));
            }

            FslibWrapper::AtomicStoreE(schemaFile, strSchema);
            bool hasException = false;
            try {
                mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_TEMP_DATA_PATH(), "tmpSchema.json");
            } catch (const util::SchemaException& e) {
                hasException = true;
                string errMsg = "index cannot configure";
                ASSERT_TRUE(e.ToString().find(errMsg) != string::npos);
            }

            if (i < 2 && strIndexType == "STRING") {
                ASSERT_TRUE(!hasException);
            } else {
                ASSERT_TRUE(hasException);
            }
        }

        if (FslibWrapper::IsExist(schemaFile).GetOrThrow()) {
            FslibWrapper::DeleteFileE(schemaFile, DeleteOption::NoFence(false));
        }
    }

    void TestCaseForLoadSchemaFailForNoPosListWithPosPayload()
    {
        bool hasException = false;
        try {
            mIndexPartitionSchema = SchemaLoader::LoadSchema(
                GET_PRIVATE_TEST_DATA_PATH(), "index_schema_for_test_no_position_list_with_position_payload.json");
        } catch (const util::SchemaException& e) {
            hasException = true;
            string errMsg = "position_payload_flag should not be set while position_list_flag is not set";
            ASSERT_TRUE(e.ToString().find(errMsg) != string::npos);
        }
        ASSERT_TRUE(hasException);
    }

    void TestCaseForConfigureOptionFlags()
    {
        mIndexPartitionSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");

        {
            IndexSchemaPtr indexSchema = mIndexPartitionSchema->GetIndexSchema();
            uint32_t count = indexSchema->GetIndexCount();
            for (uint32_t i = 0; i < count; ++i) {
                IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(i);
                InvertedIndexType type = indexConfig->GetInvertedIndexType();
                if (type == it_pack || type == it_text) {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), OPTION_FLAG_ALL);
                } else if (type == it_expack) {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), EXPACK_OPTION_FLAG_ALL);
                } else if (type == it_string) {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), NO_POSITION_LIST);
                } else if (type == it_number || type == it_number_int8 || type == it_number_uint8 ||
                           type == it_number_int16 || type == it_number_uint16 || type == it_number_int32 ||
                           type == it_number_uint32 || type == it_number_int64 || type == it_number_uint64) {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), NO_POSITION_LIST);
                } else {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), of_none);
                }
            }
        }
    }

    void TestCaseForConfigureDefaultOptionFlags()
    {
        mIndexPartitionSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "default_index_engine_schema.json");

        {
            IndexSchemaPtr indexSchema = mIndexPartitionSchema->GetIndexSchema();
            uint32_t count = indexSchema->GetIndexCount();
            for (uint32_t i = 0; i < count; ++i) {
                IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(i);
                InvertedIndexType type = indexConfig->GetInvertedIndexType();
                if (type == it_pack || type == it_text) {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), of_position_list | of_term_frequency);
                } else if (type == it_expack) {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), of_position_list | of_term_frequency | of_fieldmap);
                } else if (type != it_primarykey64 && type != it_primarykey128) {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), of_term_frequency);
                } else {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), of_none);
                }
            }
        }
    }

    void TestCaseForConfigureSpecialOptionFlags()
    {
        mIndexPartitionSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "special_index_engine_schema.json");

        {
            IndexSchemaPtr indexSchema = mIndexPartitionSchema->GetIndexSchema();
            uint32_t count = indexSchema->GetIndexCount();
            for (uint32_t i = 0; i < count; ++i) {
                IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(i);
                if (indexConfig->GetInvertedIndexType() != it_expack) {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), NO_POSITION_LIST);
                } else {
                    ASSERT_EQ(indexConfig->GetOptionFlag(), EXPACK_NO_POSITION_LIST);
                }
            }
        }
    }

    void TestCaseForCheckIndexTypeOfIndexConfig()
    {
        IndexConfigPtr indexConfig(new SingleFieldIndexConfig("test", it_number));
        ASSERT_TRUE(indexConfig->CheckFieldType(ft_uint8));
        ASSERT_TRUE(indexConfig->CheckFieldType(ft_int16));
        ASSERT_TRUE(indexConfig->CheckFieldType(ft_uint16));
        ASSERT_TRUE(indexConfig->CheckFieldType(ft_integer));
        ASSERT_TRUE(indexConfig->CheckFieldType(ft_uint32));
        ASSERT_TRUE(indexConfig->CheckFieldType(ft_long));
        ASSERT_TRUE(indexConfig->CheckFieldType(ft_uint64));
        ASSERT_FALSE(indexConfig->CheckFieldType(ft_unknown));
        ASSERT_FALSE(indexConfig->CheckFieldType(ft_online));
    }

    void TestCaseForIndexConfigNotEqual()
    {
        IndexPartitionSchemaPtr schema1;
        IndexPartitionSchemaPtr schema2;

        schema1 = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "default_index_engine_schema.json");
        IndexSchemaPtr indexSchema = schema1->GetIndexSchema();
        for (uint32_t i = 0; i < indexSchema->GetIndexCount(); ++i) {
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(i);
            InvertedIndexType indexType = indexConfig->GetInvertedIndexType();
            if (indexType == it_primarykey64 || indexType == it_primarykey128) {
                ASSERT_EQ(of_none, indexConfig->GetOptionFlag());
            } else if (indexType == it_string) {
                ASSERT_EQ(of_term_frequency, indexConfig->GetOptionFlag());
            } else if (indexType == it_number || indexType == it_number_int8 || indexType == it_number_uint8 ||
                       indexType == it_number_int16 || indexType == it_number_uint16 || indexType == it_number_int32 ||
                       indexType == it_number_uint32 || indexType == it_number_int64 || indexType == it_number_uint64) {
                ASSERT_EQ(of_term_frequency, indexConfig->GetOptionFlag());
            } else if (indexType == it_expack) {
                ASSERT_EQ(of_position_list | of_term_frequency | of_fieldmap, indexConfig->GetOptionFlag());
            } else {
                ASSERT_EQ(of_position_list | of_term_frequency, indexConfig->GetOptionFlag());
            }
        }

        schema2 = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");
        indexSchema = schema2->GetIndexSchema();
        for (uint32_t i = 0; i < indexSchema->GetIndexCount(); ++i) {
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(i);
            InvertedIndexType indexType = indexConfig->GetInvertedIndexType();
            if (indexType == it_primarykey64 || indexType == it_primarykey128) {
                ASSERT_EQ(of_none, indexConfig->GetOptionFlag());
            } else if (indexType == it_string) {
                ASSERT_EQ(NO_POSITION_LIST, indexConfig->GetOptionFlag());
            } else if (indexType == it_number || indexType == it_number_int8 || indexType == it_number_uint8 ||
                       indexType == it_number_int16 || indexType == it_number_uint16 || indexType == it_number_int32 ||
                       indexType == it_number_uint32 || indexType == it_number_int64 || indexType == it_number_uint64) {
                ASSERT_EQ(NO_POSITION_LIST, indexConfig->GetOptionFlag());
            } else if (indexType == it_expack) {
                ASSERT_EQ(EXPACK_OPTION_FLAG_ALL, indexConfig->GetOptionFlag());
            } else {
                ASSERT_EQ(OPTION_FLAG_ALL, indexConfig->GetOptionFlag());
            }
        }

        bool hasException = false;
        try {
            schema1->AssertEqual(*schema2);
        } catch (const util::AssertEqualException& e) {
            hasException = true;
            string errMsg = "mNameToIdMap not equal";
            ASSERT_TRUE(e.ToString().find(errMsg) != string::npos) << e.ToString();
        }

        ASSERT_TRUE(hasException);
    }

    void TestCaseForDefaultTermFrequencyFlag()
    {
        IndexPartitionSchemaPtr schema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example.json");

        IndexSchemaPtr indexSchema = schema->GetIndexSchema();
        for (uint32_t i = 0; i < indexSchema->GetIndexCount(); i++) {
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(i);
            optionflag_t optionFlag = indexConfig->GetOptionFlag();
            if (indexConfig->GetInvertedIndexType() != it_primarykey128 &&
                indexConfig->GetInvertedIndexType() != it_primarykey64) {
                ASSERT_TRUE(optionFlag & of_term_frequency);
            }
        }
    }

    void TestCaseForClearTermFrequencyFlag()
    {
        IndexPartitionSchemaPtr schema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "clear_tf_example.json");

        IndexSchemaPtr indexSchema = schema->GetIndexSchema();
        for (uint32_t i = 0; i < indexSchema->GetIndexCount(); i++) {
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(i);
            optionflag_t optionFlag = indexConfig->GetOptionFlag();
            ASSERT_FALSE(optionFlag & of_term_frequency);
        }
    }

    void TestCaseForClearTFWithPositionListFlagSet()
    {
        ASSERT_THROW(SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "bad_tf_flag_example.json"),
                     SchemaException);
    }

    void TestCaseForIndexConfigClone()
    {
        mIndexPartitionSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "default_index_engine_schema.json");

        const IndexSchemaPtr& indexSchema = mIndexPartitionSchema->GetIndexSchema();
        IndexConfigPtr textIndexConfig = indexSchema->GetIndexConfig("title");
        IndexConfigPtr clonedIndexConfig(textIndexConfig->Clone());
        textIndexConfig->AssertEqual(*(clonedIndexConfig.get()));

        IndexConfigPtr packIndexConfig = indexSchema->GetIndexConfig("phrase");
        clonedIndexConfig.reset(packIndexConfig->Clone());
        packIndexConfig->AssertEqual(*(clonedIndexConfig.get()));
    }

    void TestCaseForNeedStoreSummary()
    {
        string fieldSchema = "string1:string;string2:string;";
        string indexSchema = "";
        string attributeSchema = "";
        string summarySchema = "string1;string2";
        InnerTestNeedStoreSummary(fieldSchema, indexSchema, attributeSchema, summarySchema, true);

        summarySchema = "";
        InnerTestNeedStoreSummary(fieldSchema, indexSchema, attributeSchema, summarySchema, false);

        attributeSchema = "string1";
        summarySchema = "string1;string2";
        InnerTestNeedStoreSummary(fieldSchema, indexSchema, attributeSchema, summarySchema, true);

        attributeSchema = "string1;string2";
        summarySchema = "string1;string2";
        InnerTestNeedStoreSummary(fieldSchema, indexSchema, attributeSchema, summarySchema, false);
    }

    void InnerTestNeedStoreSummary(const string& fieldSchema, const string& indexSchema, const string& attributeSchema,
                                   const string& summarySchema, bool needStoreSummary)
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema, fieldSchema, indexSchema, attributeSchema, summarySchema);
        ASSERT_EQ(needStoreSummary, schema->NeedStoreSummary());
    }

protected:
    void CheckOptionFlags(optionflag_t optionFlag)
    {
        IndexSchemaPtr indexSchema = mIndexPartitionSchema->GetIndexSchema();
        uint32_t count = indexSchema->GetIndexCount();
        for (uint32_t i = 0; i < count; ++i) {
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(i);
            ASSERT_EQ(indexConfig->GetOptionFlag(), optionFlag);
        }
    }

    void CheckShardingIndexConfig(const IndexSchemaPtr& indexSchema, const string& indexName,
                                  const string& truncProfileName, size_t expectShardingCount)
    {
        string targetIndexName = indexName;
        if (!truncProfileName.empty()) {
            targetIndexName = IndexConfig::CreateTruncateIndexName(indexName, truncProfileName);
        }

        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(targetIndexName);
        ASSERT_TRUE(indexConfig);
        ASSERT_EQ(IndexConfig::IST_NEED_SHARDING, indexConfig->GetShardingType());

        vector<IndexConfigPtr> shardingIndexConfigs = indexConfig->GetShardingIndexConfigs();
        ASSERT_EQ(expectShardingCount, shardingIndexConfigs.size()) << targetIndexName;

        for (size_t i = 0; i < shardingIndexConfigs.size(); i++) {
            string shardingIndexName = IndexConfig::GetShardingIndexName(indexName, i);
            if (!truncProfileName.empty()) {
                shardingIndexName = IndexConfig::CreateTruncateIndexName(shardingIndexName, truncProfileName);
            }
            ASSERT_EQ(shardingIndexName, shardingIndexConfigs[i]->GetIndexName());
            ASSERT_EQ(IndexConfig::IST_IS_SHARDING, shardingIndexConfigs[i]->GetShardingType());
            ASSERT_EQ(shardingIndexConfigs[i], indexSchema->GetIndexConfig(shardingIndexName));

            ASSERT_EQ(indexConfig->GetDictConfig(), shardingIndexConfigs[i]->GetDictConfig());
            ASSERT_EQ(indexConfig->GetAdaptiveDictionaryConfig(),
                      shardingIndexConfigs[i]->GetAdaptiveDictionaryConfig());
            ASSERT_EQ(indexConfig->GetOptionFlag(), shardingIndexConfigs[i]->GetOptionFlag());
        }
    }

    void TestCaseForSubSchema()
    {
        mIndexPartitionSchema =
            SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "index_engine_example_with_sub_schema.json");
        EXPECT_TRUE(NULL != mIndexPartitionSchema->GetSubIndexPartitionSchema());
        EXPECT_TRUE(mIndexPartitionSchema->IsUsefulField(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME));
        ASSERT_TRUE(mIndexPartitionSchema->GetAttributeSchema()
                        ->GetAttributeConfig(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME)
                        ->IsBuiltInAttribute());

        IndexPartitionSchemaPtr subSchema = mIndexPartitionSchema->GetSubIndexPartitionSchema();
        EXPECT_TRUE(subSchema->IsUsefulField(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME));
        ASSERT_TRUE(subSchema->GetAttributeSchema()
                        ->GetAttributeConfig(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME)
                        ->IsBuiltInAttribute());
    }

private:
    IndexPartitionSchemaPtr mIndexPartitionSchema;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.config, SchemaConfiguratorTest);

INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForIndexPartitionSchema);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForIndexAnalyzer);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForTableType);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForIndexAnalyzerException);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForAdaptiveDictionarySchema);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForDictionarySchema);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForEmptyDictionarySchema);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForFieldSchemaUpdatableMultiValue);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForFieldSchema);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForIndexSchema);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForIndexSchemaPostingTypeWithoutDictInPack);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForAttributeSchemeErrorSortTag);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForAttributeSchema);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForSummarySchema);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForFieldTypeToStr);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForStrToFieldType);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForInvertedIndexTypeToStr);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForStrToIndexType);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForLoadSchemaFailForNoPosListWithPosPayload);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForConfigureOptionFlags);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForConfigureDefaultOptionFlags);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForConfigureSpecialOptionFlags);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForIndexConfigNotEqual);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForLoadSchemaFailForStringPKIndexWithOptionFlag);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForCheckIndexTypeOfIndexConfig);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForCheckOptionFlagForNumberIndex);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForCheckOptionFlagForPKIndex);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForDefaultTermFrequencyFlag);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForClearTermFrequencyFlag);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForClearTFWithPositionListFlagSet);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForIndexConfigClone);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForNeedStoreSummary);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForShardingIndex);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForShardingIndexException);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForDotCharInFieldNameException);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForSubSchema);
INDEXLIB_UNIT_TEST_CASE(SchemaConfiguratorTest, TestCaseForSeparator);
}} // namespace indexlib::config
