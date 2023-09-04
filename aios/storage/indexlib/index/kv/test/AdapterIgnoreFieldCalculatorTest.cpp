#include "indexlib/index/kv/AdapterIgnoreFieldCalculator.h"

#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/framework/mock/FakeSegment.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace index {

class FakeTabletDataCreator
{
public:
    FakeTabletDataCreator() { _schemaGroup.reset(new framework::TabletDataSchemaGroup); }

    FakeTabletDataCreator& RegisterSchema(const std::shared_ptr<config::ITabletSchema>& schema)
    {
        _schemaGroup->multiVersionSchemas[schema->GetSchemaId()] = schema;
        _schemaGroup->writeSchema = _schemaGroup->multiVersionSchemas.rbegin()->second;
        _schemaGroup->onDiskWriteSchema = _schemaGroup->multiVersionSchemas.rbegin()->second;
        _version.SetSchemaId(_schemaGroup->writeSchema->GetSchemaId());
        return *this;
    }

    FakeTabletDataCreator& AddSegment(segmentid_t segId, schemaid_t segmentSchemaId)
    {
        auto segment = make_shared<framework::FakeSegment>(framework::Segment::SegmentStatus::ST_BUILT);
        framework::SegmentMeta meta(segId);
        auto iter = _schemaGroup->multiVersionSchemas.find(segmentSchemaId);
        assert(iter != _schemaGroup->multiVersionSchemas.end());
        meta.schema = iter->second;
        segment->TEST_SetSegmentMeta(meta);
        _segments.push_back(segment);
        _version.AddSegment(segId);
        _version.UpdateSegmentSchemaId(segId, segmentSchemaId);
        return *this;
    }

    FakeTabletDataCreator& SetReadSchemaId(schemaid_t schemaId)
    {
        auto iter = _schemaGroup->multiVersionSchemas.find(schemaId);
        assert(iter != _schemaGroup->multiVersionSchemas.end());
        _schemaGroup->onDiskReadSchema = iter->second;
        _version.SetReadSchemaId(schemaId);
        return *this;
    }

    std::shared_ptr<framework::TabletData> CreateTabletData()
    {
        if (!_schemaGroup->onDiskReadSchema && _schemaGroup->onDiskWriteSchema) {
            _schemaGroup->onDiskReadSchema = _schemaGroup->onDiskWriteSchema;
            _version.SetReadSchemaId(_schemaGroup->onDiskReadSchema->GetSchemaId());
        }
        _version.SetVersionId(0);
        auto resourceMap = std::make_shared<framework::ResourceMap>();
        [[maybe_unused]] auto st =
            resourceMap->AddVersionResource(framework::TabletDataSchemaGroup::NAME, _schemaGroup);
        assert(st.IsOK());
        auto tabletData = std::make_shared<framework::TabletData>("test_table");
        st = tabletData->Init(_version, _segments, resourceMap);
        if (st.IsOK()) {
            return tabletData;
        }
        assert(false);
        return std::shared_ptr<framework::TabletData>();
    }

private:
    std::vector<std::shared_ptr<framework::Segment>> _segments;
    framework::Version _version;
    std::shared_ptr<framework::TabletDataSchemaGroup> _schemaGroup;
};

class AdapterIgnoreFieldCalculatorTest : public TESTBASE
{
public:
    AdapterIgnoreFieldCalculatorTest();
    ~AdapterIgnoreFieldCalculatorTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    std::shared_ptr<config::ITabletSchema> MakeSchema(const std::string& fields, const std::string& attributes,
                                                      schemaid_t schemaId)
    {
        auto schema = table::KVTabletSchemaMaker::Make(fields, "pk", attributes);
        schema->SetSchemaId(schemaId);
        return schema;
    }

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, AdapterIgnoreFieldCalculatorTest);

AdapterIgnoreFieldCalculatorTest::AdapterIgnoreFieldCalculatorTest() {}

AdapterIgnoreFieldCalculatorTest::~AdapterIgnoreFieldCalculatorTest() {}

TEST_F(AdapterIgnoreFieldCalculatorTest, TestSimpleProcess)
{
    FakeTabletDataCreator creator;
    auto tabletData =
        creator.RegisterSchema(MakeSchema("pk:string;value:uint32;price:float", "value;price", 0))
            .RegisterSchema(MakeSchema("pk:string;value:uint32;price:float;category:string", "value;price;category",
                                       1))                                                             // add category
            .RegisterSchema(MakeSchema("pk:string;value:uint32;category:string", "value;category", 2)) // delete price
            .RegisterSchema(MakeSchema("pk:string;value:uint32;category:string;string2:string",
                                       "value;category;string2", 3))                                   // add string2
            .RegisterSchema(MakeSchema("pk:string;value:uint32;category:string", "value;category", 4)) // delete string2
            .RegisterSchema(MakeSchema("pk:string;value:uint32;category:string;price:double", "value;category;price",
                                       5)) // add price
            .RegisterSchema(MakeSchema("pk:string;value:uint32;category:string;string2:string",
                                       "value;category;string2", 6)) // delete price & add string2
            .AddSegment(0, 0)
            .AddSegment(1, 1)
            .AddSegment(2, 2)
            .AddSegment(3, 3)
            .AddSegment(4, 4)
            .AddSegment(5, 5)
            .AddSegment(6, 6)
            .CreateTabletData();

    auto kvIndexConfig = dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(
        tabletData->GetWriteSchema()->GetIndexConfig("kv", "pk"));

    AdapterIgnoreFieldCalculator calculator;
    ASSERT_TRUE(calculator.Init(kvIndexConfig, tabletData.get()));

    auto CheckIgnoreFields = [&](schemaid_t begin, schemaid_t end, const std::string& fields) {
        auto ignoreFields = calculator.GetIgnoreFields(begin, end);
        ASSERT_EQ(fields, autil::StringUtil::toString(ignoreFields, ","));
    };

    CheckIgnoreFields(0, 1, "");
    CheckIgnoreFields(0, 2, "");
    CheckIgnoreFields(0, 3, "price");
    CheckIgnoreFields(0, 4, "price");
    CheckIgnoreFields(0, 5, "price,string2");
    CheckIgnoreFields(0, 6, "price,string2");

    CheckIgnoreFields(2, 4, "");
    CheckIgnoreFields(2, 5, "string2");
}

}} // namespace indexlibv2::index
