#include "build_service/test/unittest.h"
#include "build_service/tools/partition_split_merger/PartitionSplitChecker.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/IndexPartitionOptionsWrapper.h"
#include <indexlib/index_base/schema_adapter.h>

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

using namespace build_service::config;

namespace build_service {
namespace tools {

class PartitionSplitCheckerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp() {
        _param.configPath = TEST_DATA_PATH"partition_split_checker";
        _param.clusterName = "mainse_searcher";
    }
public:
    PartitionSplitChecker::Param _param;
};

TEST_F(PartitionSplitCheckerTest, testCheck) {
    {
        // check adaptive bitmap
        PartitionSplitChecker checker;
        ASSERT_TRUE(checker.init(_param));
        IndexSchemaPtr indexSchema =
            checker._indexPartitionSchemaPtr->GetIndexSchema();
        for (size_t i = 0; i < indexSchema->GetIndexCount(); ++i) {
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig((indexid_t)i);
            if (indexConfig)
            {
                indexConfig->SetHasTruncateFlag(false);
            }
        }
        EXPECT_FALSE(checker.check());
    }
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ResourceReaderPtr resourceReader =
            ResourceReaderManager::getResourceReader(_param.configPath);
        ASSERT_TRUE(resourceReader->getConfigWithJsonPath(
                        "schemas/searcher_test_schema.json", "", schema));
        PartitionSplitChecker checker;
        checker._indexPartitionSchemaPtr = schema;
        EXPECT_TRUE(checker.check());
    }
    {
        // check truncate index
        PartitionSplitChecker checker;
        ASSERT_TRUE(checker.init(_param));
        IndexSchemaPtr indexSchema = checker._indexPartitionSchemaPtr->GetIndexSchema();
        for (size_t i = 0; i < indexSchema->GetIndexCount(); ++i) {
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig((indexid_t)i);
            if (indexConfig && indexConfig->HasAdaptiveDictionary()) {
                indexConfig->SetAdaptiveDictConfig(AdaptiveDictionaryConfigPtr());
            }
        }
        EXPECT_FALSE(checker.check());
    }
}

TEST_F(PartitionSplitCheckerTest, testInitIndexPartitionSchema) {
    PartitionSplitChecker::Param param;
    PartitionSplitChecker checker;
    {
        //success
        param.configPath = _param.configPath;
        param.clusterName = "mainse_searcher";
        EXPECT_TRUE(checker.initIndexPartitionSchema(param));
    }
    {
        //fail
        PartitionSplitChecker::Param tmpParam;
        tmpParam = param;
        tmpParam.clusterName = "notexist";
        EXPECT_TRUE(!checker.initIndexPartitionSchema(tmpParam));
    }
}

}
}

