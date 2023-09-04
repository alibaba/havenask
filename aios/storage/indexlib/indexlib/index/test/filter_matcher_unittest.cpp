#include <typeinfo>

#include "indexlib/index/segment_metrics_updater/equal_filter_matcher.h"
#include "indexlib/index/segment_metrics_updater/filter_matcher_creator.h"
#include "indexlib/index/segment_metrics_updater/range_filter_matcher.h"
#include "indexlib/misc/common.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/unittest.h"

using namespace std;

using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace index {

class FilterMatcherTest : public INDEXLIB_TESTBASE
{
public:
    FilterMatcherTest();
    ~FilterMatcherTest();

    DECLARE_CLASS_NAME(FilterMatcherTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestEqualFilterMatcher();
    void TestRangeFilterMatcher();

private:
    void CheckValue(const FilterMatcherPtr& matcher, const document::DocumentPtr& doc, bool isEmpty);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FilterMatcherTest, TestEqualFilterMatcher);
INDEXLIB_UNIT_TEST_CASE(FilterMatcherTest, TestRangeFilterMatcher);
IE_LOG_SETUP(index, FilterMatcherTest);

FilterMatcherTest::FilterMatcherTest() {}

FilterMatcherTest::~FilterMatcherTest() {}

void FilterMatcherTest::CaseSetUp() {}

void FilterMatcherTest::CaseTearDown() {}

void FilterMatcherTest::CheckValue(const FilterMatcherPtr& matcher, const document::DocumentPtr& doc, bool isEmpty)
{
    vector<int32_t> res;
    matcher->Match(doc, res);
    if (isEmpty) {
        ASSERT_TRUE(res.empty());
    } else {
        ASSERT_EQ(1, res.size());
        ASSERT_EQ(0, res[0]);
    }
}

void FilterMatcherTest::TestEqualFilterMatcher()
{
    {
        // test string,string
        string field = "pk:string:pk;string1:string:false:true;";
        string index = "pk:primarykey64:pk";
        string attr = "string1";
        auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
        auto attrSchema = schema->GetAttributeSchema();
        string filterStr = R"({"type" : "Equal", "field_name" : "string1",  "value" : "22", "value_type" : "STRING"} )";
        ConditionFilterPtr filter(new ConditionFilter);
        FromJsonString(*filter, filterStr);
        FilterMatcherPtr matcher = FilterMatcherCreator::Create(attrSchema, filter, 0);
        auto& matcher_raw = *matcher;
        ASSERT_EQ(typeid(EqualFilterMatcher<string, string>), typeid(matcher_raw));
        matcher->Init(attrSchema, filter, 0);
        IndexPartitionOptions option;
        PartitionStateMachine psm;
        psm.Init(schema, option, GET_TEMP_DATA_PATH());
        string docStr = "cmd=add,pk=22,string1=hello2;"
                        "cmd=add,pk=22,string1=22;";
        auto docs = psm.CreateDocuments(docStr);
        ASSERT_EQ(2, docs.size());
        CheckValue(matcher, docs[0], true);
        CheckValue(matcher, docs[1], false);
    }
    {
        // test int32_t, int64_t
        string field = "pk:string:pk;int1:int32;";
        string index = "pk:primarykey64:pk";
        string attr = "int1";
        auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
        auto attrSchema = schema->GetAttributeSchema();
        string filterStr = R"({"type" : "Equal", "field_name" : "int1",  "value" : "22", "value_type" : "int64"} )";
        ConditionFilterPtr filter(new ConditionFilter);
        FromJsonString(*filter, filterStr);
        FilterMatcherPtr matcher = FilterMatcherCreator::Create(attrSchema, filter, 0);
        auto& matcher_raw = *matcher;
        ASSERT_EQ(typeid(EqualFilterMatcher<int32_t, int64_t>), typeid(matcher_raw));
        matcher->Init(attrSchema, filter, 0);
        IndexPartitionOptions option;
        PartitionStateMachine psm;
        psm.Init(schema, option, GET_TEMP_DATA_PATH());
        string docStr = "cmd=add,pk=22,int1=2;"
                        "cmd=add,pk=22,int1=22;";
        auto docs = psm.CreateDocuments(docStr);
        ASSERT_EQ(2, docs.size());
        CheckValue(matcher, docs[0], true);
        CheckValue(matcher, docs[1], false);
    }
    {
        // test uint32_t, uint64_t
        string field = "pk:string:pk;int1:uint32;";
        string index = "pk:primarykey64:pk";
        string attr = "int1";
        auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
        auto attrSchema = schema->GetAttributeSchema();
        string filterStr = R"({"type" : "Equal", "field_name" : "int1",  "value" : "22", "value_type" : "uint64"} )";
        ConditionFilterPtr filter(new ConditionFilter);
        FromJsonString(*filter, filterStr);
        FilterMatcherPtr matcher = FilterMatcherCreator::Create(attrSchema, filter, 0);
        auto& matcher_raw = *matcher;
        ASSERT_EQ(typeid(EqualFilterMatcher<uint32_t, uint64_t>), typeid(matcher_raw));
        matcher->Init(attrSchema, filter, 0);
        IndexPartitionOptions option;
        PartitionStateMachine psm;
        psm.Init(schema, option, GET_TEMP_DATA_PATH());
        string docStr = "cmd=add,pk=22,int1=2;"
                        "cmd=add,pk=22,int1=22;";
        auto docs = psm.CreateDocuments(docStr);
        ASSERT_EQ(2, docs.size());
        CheckValue(matcher, docs[0], true);
        CheckValue(matcher, docs[1], false);
    }
    {
        // test double, double
        string field = "pk:string:pk;double1:double;";
        string index = "pk:primarykey64:pk";
        string attr = "double1";
        auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
        auto attrSchema = schema->GetAttributeSchema();
        string filterStr =
            R"({"type" : "Equal", "field_name" : "double1",  "value" : "22.20", "value_type" : "double"} )";
        ConditionFilterPtr filter(new ConditionFilter);
        FromJsonString(*filter, filterStr);
        FilterMatcherPtr matcher = FilterMatcherCreator::Create(attrSchema, filter, 0);
        auto& matcher_raw = *matcher;
        ASSERT_EQ(typeid(EqualFilterMatcher<double, double>), typeid(matcher_raw));
        matcher->Init(attrSchema, filter, 0);
        IndexPartitionOptions option;
        PartitionStateMachine psm;
        psm.Init(schema, option, GET_TEMP_DATA_PATH());
        string docStr = "cmd=add,pk=22,double1=2.2;"
                        "cmd=add,pk=22,double1=22.2;"
                        "cmd=add,pk=22,double1=22.200000011;"
                        "cmd=add,pk=22,double1=22.200000009;";
        auto docs = psm.CreateDocuments(docStr);
        ASSERT_EQ(4, docs.size());
        CheckValue(matcher, docs[0], true);
        CheckValue(matcher, docs[1], false);
        CheckValue(matcher, docs[2], true);
        CheckValue(matcher, docs[3], false);
    }
    {
        // test int32_t, double
        string field = "pk:string:pk;int1:int32;";
        string index = "pk:primarykey64:pk";
        string attr = "int1";
        auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
        auto attrSchema = schema->GetAttributeSchema();
        string filterStr = R"({"type" : "Equal", "field_name" : "int1",  "value" : "22", "value_type" : "double"} )";
        ConditionFilterPtr filter(new ConditionFilter);
        FromJsonString(*filter, filterStr);
        FilterMatcherPtr matcher = FilterMatcherCreator::Create(attrSchema, filter, 0);
        auto& matcher_raw = *matcher;
        ASSERT_EQ(typeid(EqualFilterMatcher<int32_t, double>), typeid(matcher_raw));
        matcher->Init(attrSchema, filter, 0);
        IndexPartitionOptions option;
        PartitionStateMachine psm;
        psm.Init(schema, option, GET_TEMP_DATA_PATH());
        string docStr = "cmd=add,pk=22,int1=2;"
                        "cmd=add,pk=22,int1=22;";
        auto docs = psm.CreateDocuments(docStr);
        ASSERT_EQ(2, docs.size());
        CheckValue(matcher, docs[0], true);
        CheckValue(matcher, docs[1], false);
    }
}

void FilterMatcherTest::TestRangeFilterMatcher()
{
    {
        // test int32_t, int64_t (,]
        string field = "pk:string:pk;int1:int32;";
        string index = "pk:primarykey64:pk";
        string attr = "int1";
        auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
        auto attrSchema = schema->GetAttributeSchema();
        string filterStr =
            R"({"type" : "Range", "field_name" : "int1",  "value" : "(5, 80]", "value_type" : "int64"} )";
        ConditionFilterPtr filter(new ConditionFilter);
        FromJsonString(*filter, filterStr);
        FilterMatcherPtr matcher = FilterMatcherCreator::Create(attrSchema, filter, 0);
        auto& matcher_raw = *matcher;
        ASSERT_EQ(typeid(RangeFilterMatcher<int32_t, int64_t>), typeid(matcher_raw));
        matcher->Init(attrSchema, filter, 0);
        IndexPartitionOptions option;
        PartitionStateMachine psm;
        psm.Init(schema, option, GET_TEMP_DATA_PATH());
        string docStr = "cmd=add,pk=22,int1=2;"
                        "cmd=add,pk=22,int1=5;"
                        "cmd=add,pk=22,int1=22;"
                        "cmd=add,pk=22,int1=80;"
                        "cmd=add,pk=22,int1=81;";
        auto docs = psm.CreateDocuments(docStr);
        ASSERT_EQ(5, docs.size());
        CheckValue(matcher, docs[0], true);
        CheckValue(matcher, docs[1], true);
        CheckValue(matcher, docs[2], false);
        CheckValue(matcher, docs[3], false);
        CheckValue(matcher, docs[4], true);
    }
    {
        // test int32_t, int64_t [,)
        string field = "pk:string:pk;int1:int32;";
        string index = "pk:primarykey64:pk";
        string attr = "int1";
        auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
        auto attrSchema = schema->GetAttributeSchema();
        string filterStr =
            R"({"type" : "Range", "field_name" : "int1",  "value" : "[5, 80) ", "value_type" : "int64"} )";
        ConditionFilterPtr filter(new ConditionFilter);
        FromJsonString(*filter, filterStr);
        FilterMatcherPtr matcher = FilterMatcherCreator::Create(attrSchema, filter, 0);
        auto& matcher_raw = *matcher;
        ASSERT_EQ(typeid(RangeFilterMatcher<int32_t, int64_t>), typeid(matcher_raw));
        matcher->Init(attrSchema, filter, 0);
        IndexPartitionOptions option;
        PartitionStateMachine psm;
        psm.Init(schema, option, GET_TEMP_DATA_PATH());
        string docStr = "cmd=add,pk=22,int1=2;"
                        "cmd=add,pk=22,int1=5;"
                        "cmd=add,pk=22,int1=22;"
                        "cmd=add,pk=22,int1=80;"
                        "cmd=add,pk=22,int1=81;";
        auto docs = psm.CreateDocuments(docStr);
        ASSERT_EQ(5, docs.size());
        CheckValue(matcher, docs[0], true);
        CheckValue(matcher, docs[1], false);
        CheckValue(matcher, docs[2], false);
        CheckValue(matcher, docs[3], true);
        CheckValue(matcher, docs[4], true);
    }
    {
        // test uint32_t, uint64_t (,]
        string field = "pk:string:pk;int1:uint32;";
        string index = "pk:primarykey64:pk";
        string attr = "int1";
        auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
        auto attrSchema = schema->GetAttributeSchema();
        string filterStr =
            R"({"type" : "Range", "field_name" : "int1",  "value" : "(5, 80]", "value_type" : "uint64"} )";
        ConditionFilterPtr filter(new ConditionFilter);
        FromJsonString(*filter, filterStr);
        FilterMatcherPtr matcher = FilterMatcherCreator::Create(attrSchema, filter, 0);
        auto& matcher_raw = *matcher;
        ASSERT_EQ(typeid(RangeFilterMatcher<uint32_t, uint64_t>), typeid(matcher_raw));
        matcher->Init(attrSchema, filter, 0);
        IndexPartitionOptions option;
        PartitionStateMachine psm;
        psm.Init(schema, option, GET_TEMP_DATA_PATH());
        string docStr = "cmd=add,pk=22,int1=2;"
                        "cmd=add,pk=22,int1=5;"
                        "cmd=add,pk=22,int1=22;"
                        "cmd=add,pk=22,int1=80;"
                        "cmd=add,pk=22,int1=81;";
        auto docs = psm.CreateDocuments(docStr);
        ASSERT_EQ(5, docs.size());
        CheckValue(matcher, docs[0], true);
        CheckValue(matcher, docs[1], true);
        CheckValue(matcher, docs[2], false);
        CheckValue(matcher, docs[3], false);
        CheckValue(matcher, docs[4], true);
    }
    {
        // test uint32_t, double (,]
        string field = "pk:string:pk;int1:uint32;";
        string index = "pk:primarykey64:pk";
        string attr = "int1";
        auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
        auto attrSchema = schema->GetAttributeSchema();
        string filterStr =
            R"({"type" : "Range", "field_name" : "int1",  "value" : "(5, 80]", "value_type" : "double"} )";
        ConditionFilterPtr filter(new ConditionFilter);
        FromJsonString(*filter, filterStr);
        FilterMatcherPtr matcher = FilterMatcherCreator::Create(attrSchema, filter, 0);
        auto& matcher_raw = *matcher;
        ASSERT_EQ(typeid(RangeFilterMatcher<uint32_t, double>), typeid(matcher_raw));
        matcher->Init(attrSchema, filter, 0);
        IndexPartitionOptions option;
        PartitionStateMachine psm;
        psm.Init(schema, option, GET_TEMP_DATA_PATH());
        string docStr = "cmd=add,pk=22,int1=2;"
                        "cmd=add,pk=22,int1=5;"
                        "cmd=add,pk=22,int1=22;"
                        "cmd=add,pk=22,int1=80;"
                        "cmd=add,pk=22,int1=81;";
        auto docs = psm.CreateDocuments(docStr);
        ASSERT_EQ(5, docs.size());
        CheckValue(matcher, docs[0], true);
        CheckValue(matcher, docs[1], true);
        CheckValue(matcher, docs[2], false);
        CheckValue(matcher, docs[3], false);
        CheckValue(matcher, docs[4], true);
    }
    {
        // test double, double (,]
        string field = "pk:string:pk;double1:double;";
        string index = "pk:primarykey64:pk";
        string attr = "double1";
        auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
        auto attrSchema = schema->GetAttributeSchema();
        string filterStr =
            R"({"type" : "Range", "field_name" : "double1",  "value" : "(5, 80]", "value_type" : "double"} )";
        ConditionFilterPtr filter(new ConditionFilter);
        FromJsonString(*filter, filterStr);
        FilterMatcherPtr matcher = FilterMatcherCreator::Create(attrSchema, filter, 0);
        auto& matcher_raw = *matcher;
        ASSERT_EQ(typeid(RangeFilterMatcher<double, double>), typeid(matcher_raw));
        matcher->Init(attrSchema, filter, 0);
        IndexPartitionOptions option;
        PartitionStateMachine psm;
        psm.Init(schema, option, GET_TEMP_DATA_PATH());
        string docStr = "cmd=add,pk=22,double1=2;"
                        "cmd=add,pk=22,double1=5;"
                        "cmd=add,pk=22,double1=22;"
                        "cmd=add,pk=22,double1=80.000000009;"
                        "cmd=add,pk=22,double1=80.000000011;";
        auto docs = psm.CreateDocuments(docStr);
        ASSERT_EQ(5, docs.size());
        CheckValue(matcher, docs[0], true);
        CheckValue(matcher, docs[1], true);
        CheckValue(matcher, docs[2], false);
        CheckValue(matcher, docs[3], false);
        CheckValue(matcher, docs[4], true);
    }
}
}} // namespace indexlib::index
