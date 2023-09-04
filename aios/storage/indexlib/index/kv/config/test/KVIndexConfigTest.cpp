#include "indexlib/index/kv/config/KVIndexConfig.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

class KVIndexConfigTest : public TESTBASE
{
public:
    void setUp() override {};
    void tearDown() override {};
};

TEST_F(KVIndexConfigTest, TestDeserialize)
{
    auto kvIndexConfig = make_shared<KVIndexConfig>();
    auto jsonStr = R"(
    {
        "index_name" : "index1",
        "key_field"  : "key",
        "value_fields" : [
                "weights"
        ],
        "index_preference": {
            "parameters": {
                "hash_dict": {
                    "type": "cuckoo"
                }
            }
        },
        "use_number_hash":true,
        "default_ttl": 1024,
        "store_expire_time": true,
        "use_swap_mmap_file": true,
        "max_swap_mmap_file_size": 1024
    }
    )";
    auto any = ParseJson(jsonStr);

    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    fieldConfigs.emplace_back(std::make_shared<FieldConfig>("key", ft_int64, false));
    fieldConfigs.emplace_back(std::make_shared<FieldConfig>("weights", ft_float, true));
    MutableJson runtimeSettings;
    IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    ASSERT_NO_THROW(kvIndexConfig->Deserialize(any, 0, resource));
    ASSERT_EQ("index1", kvIndexConfig->GetIndexName());
    ASSERT_EQ(true, kvIndexConfig->UseNumberHash());
    ASSERT_EQ(1024, kvIndexConfig->GetTTL());
    ASSERT_EQ(true, kvIndexConfig->StoreExpireTime());
    ASSERT_EQ(true, kvIndexConfig->GetUseSwapMmapFile());
    ASSERT_EQ(1024, kvIndexConfig->GetMaxSwapMmapFileSize());

    auto indexPreference = kvIndexConfig->GetIndexPreference();
    const auto keyField = kvIndexConfig->GetFieldConfig();
    const auto valueFields = kvIndexConfig->GetValueConfig();
    ASSERT_EQ("cuckoo", indexPreference.GetHashDictParam().GetHashType());
    ASSERT_EQ("key", keyField->GetFieldName());
    ASSERT_EQ(ft_long, keyField->GetFieldType());
    ASSERT_EQ(1, valueFields->GetAttributeCount());
    const auto valueFieldConfig = valueFields->GetAttributeConfig(0)->GetFieldConfig();
    ASSERT_EQ("weights", valueFieldConfig->GetFieldName());
    ASSERT_EQ(ft_float, valueFieldConfig->GetFieldType());
    ASSERT_EQ(true, valueFieldConfig->IsMultiValue());
}

} // namespace indexlibv2::config
