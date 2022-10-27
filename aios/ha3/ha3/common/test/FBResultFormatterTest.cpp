#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/FBResultFormatter.h>
#include <ha3/common/PBResultFormatter.h>
#include <ha3/common/test/ResultConstructor.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>
#include <ha3/common/Request.h>
#include <ha3/common/test/ResultConstructor.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <indexlib/config/field_type_traits.h>

using namespace std;
using namespace flatbuffers;
using namespace isearch::fbs;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
IE_NAMESPACE_USE(config);
BEGIN_HA3_NAMESPACE(common);

class FBResultFormatterTest : public TESTBASE {
public:
    FBResultFormatterTest();
    ~FBResultFormatterTest();
public:
    void setUp();
    void tearDown();
protected:

protected:
    autil::mem_pool::Pool _pool;

protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, FBResultFormatterTest);


FBResultFormatterTest::FBResultFormatterTest() {
}

FBResultFormatterTest::~FBResultFormatterTest() {
}

void FBResultFormatterTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void FBResultFormatterTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(FBResultFormatterTest, testFormat) {
    FBResultFormatter formatter(&_pool);
    stringstream ss;
    ResultPtr resultPtr(new Result);
    TracerPtr tracer(new Tracer());
    tracer->trace("test trace info");
    resultPtr->setTracer(tracer);

    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setHostInfo("12345", "test");
    errorResult.setErrorMsg("error message");

    MultiErrorResultPtr errorResults(new MultiErrorResult);
    errorResults->addErrorResult(errorResult);

    resultPtr->addErrorResult(errorResults);
    resultPtr->setTotalTime(123456);

    string clusterName = "clustername";
    HitPtr hitPtr1(new Hit(123, 1, clusterName));
    HitPtr hitPtr2(new Hit(456, 2, clusterName));

    config::HitSummarySchema hitSummarySchema;
    hitSummarySchema.declareSummaryField("field1");
    hitSummarySchema.declareSummaryField("field2");
    hitSummarySchema.declareSummaryField("field3");

    SummaryHit *summaryHit1 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit1->setFieldValue(0, "value10", 7);
    summaryHit1->setFieldValue(1, "value11", 7);
    summaryHit1->setFieldValue(2, "", 0);
    hitPtr1->setSummaryHit(summaryHit1);

    SummaryHit *summaryHit2 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit2->setFieldValue(0, "value20", 7);
    summaryHit2->setFieldValue(1, "", 0);
    summaryHit2->setFieldValue(2, "value22", 7);
    hitPtr2->setSummaryHit(summaryHit2);

    Hits *hits = new Hits();
    hits->addHit(hitPtr1);
    hits->addHit(hitPtr2);
    hits->setTotalHits(100);
    resultPtr->setHits(hits);

    string resultStr = formatter.format(resultPtr);

    const char *data = resultStr.c_str();
    const SummaryResult *testSummaryResult = GetRoot<SummaryResult>(data);
    ASSERT_TRUE(testSummaryResult != nullptr);

    ASSERT_EQ(123456, testSummaryResult->totalTime());
    ASSERT_EQ(100, testSummaryResult->totalHits());

    const flatbuffers::String *testTracer = testSummaryResult->tracer();
    ASSERT_TRUE(testTracer != nullptr);
    ASSERT_EQ(string("test trace info\n"), testTracer->str());

    const Vector<Offset<FBErrorResult> > *testErrorResults =
        testSummaryResult->errorResults();
    ASSERT_TRUE(testErrorResults != nullptr);
    ASSERT_EQ((size_t)1, testErrorResults->size());
    const FBErrorResult *fbErrorResult = testErrorResults->Get(0);
    ASSERT_TRUE(fbErrorResult != nullptr);
    ASSERT_EQ("12345", fbErrorResult->partitionId()->str());
    ASSERT_EQ("test", fbErrorResult->hostName()->str());
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, fbErrorResult->errorDescription()->str());

    const TwoDimTable *summaryTable = testSummaryResult->summaryTable();
    ASSERT_TRUE(summaryTable != nullptr);
    const Vector<Offset<Column> > *columns = summaryTable->columns();
    ASSERT_TRUE(columns != nullptr);

    {
        const Column *column1 = columns->Get(0);
        ASSERT_TRUE(column1 != nullptr);
        const flatbuffers::String *fieldName = column1->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field1"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column1->value_type());

        const StringColumn *stringColumn = column1->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value10"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value20"), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column2 = columns->Get(1);
        ASSERT_TRUE(column2 != nullptr);
        const flatbuffers::String *fieldName = column2->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field2"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column2->value_type());

        const StringColumn *stringColumn = column2->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value11"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string(""), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column3 = columns->Get(2);
        ASSERT_TRUE(column3 != nullptr);
        const flatbuffers::String *fieldName = column3->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field3"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column3->value_type());

        const StringColumn *stringColumn = column3->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string(""), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value22"), stringColumnValue->Get(1)->str());
    }
}

TEST_F(FBResultFormatterTest, testCreateErrorResults) {
    FBResultFormatter formatter(&_pool);
    ErrorResult result1(ERROR_UNKNOWN);
    result1.setHostInfo("12345", "test");
    result1.setErrorMsg("error message");
    ErrorResult result2;
    ErrorResult result3;

    MultiErrorResultPtr errorResults(new MultiErrorResult);
    errorResults->addErrorResult(result1);
    errorResults->addErrorResult(result2);
    errorResults->addErrorResult(result3);

    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    Offset<Vector<Offset<FBErrorResult> > > fbErrorResults
        = formatter.CreateErrorResults(errorResults, fbb);
    fbb.Finish(fbErrorResults);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());

    const Vector<Offset<FBErrorResult> > *testErrorResults = GetRoot<Vector<Offset<FBErrorResult> > >(data);
    ASSERT_TRUE(testErrorResults != nullptr);
    ASSERT_EQ(1, testErrorResults->size());

    const flatbuffers::String * partId1 = testErrorResults->Get(0)->partitionId();
    ASSERT_TRUE(partId1 != nullptr);
    ASSERT_EQ(string("12345"), partId1->str());

    const flatbuffers::String * hostName1 = testErrorResults->Get(0)->hostName();
    ASSERT_TRUE(hostName1 != nullptr);
    ASSERT_EQ(string("test"), hostName1->str());

    const flatbuffers::String * errorDescription1 = testErrorResults->Get(0)->errorDescription();
    ASSERT_TRUE(errorDescription1 != nullptr);
    string expectErrorMsg1 = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg1, errorDescription1->str());

    ASSERT_EQ(ERROR_UNKNOWN, testErrorResults->Get(0)->errorCode());
}

TEST_F(FBResultFormatterTest, testCreateTracer) {
    FBResultFormatter formatter(&_pool);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);

    Tracer tracer;
    tracer.trace("test trace info");

    Offset<flatbuffers::String> fbTracer = formatter.CreateTracer(&tracer, fbb);
    fbb.Finish(fbTracer);

    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const flatbuffers::String *testTracer = GetRoot<flatbuffers::String> (data);
    ASSERT_TRUE(testTracer != nullptr);

    ASSERT_EQ(string("test trace info\n"), testTracer->str());
}


#define DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(fieldType, columnType)                     \
    template<>                                                          \
    Offset<Column> createSummaryTableColumn<fieldType>(                 \
            const char *fieldName,                                      \
            int index,                                                  \
            vector<HitPtr> &hitVec,                                     \
            FlatBufferBuilder &fbb,                                     \
            FBResultFormatter &formatter)                               \
    {                                                                   \
        return formatter.CreateSummaryTableColumnBy##columnType("field0", 0, hitVec, fbb); \
    }                                                                   \
    template<>                                                          \
    void checkColumnValue<fieldType>(const Column *testSummaryTableColumn) { \
        ASSERT_EQ(ColumnType_##columnType##Column, testSummaryTableColumn->value_type()); \
        const auto *fieldType##Column =                                 \
            testSummaryTableColumn->value_as_##columnType##Column(); \
        ASSERT_TRUE(fieldType##Column != nullptr); \
        const auto *fieldType##ColumnValue = fieldType##Column->value(); \
        ASSERT_EQ(3, fieldType##ColumnValue->size());                    \
        ASSERT_LT(std::abs((double)11 - fieldType##ColumnValue->Get(0)), 1e-5); \
        ASSERT_LT(std::abs((double)12 - fieldType##ColumnValue->Get(1)), 1e-5); \
        ASSERT_LT(std::abs((double)0 - fieldType##ColumnValue->Get(2)), 1e-5); \
    }

template <typename T>
Offset<Column> createSummaryTableColumn(
        const char *fieldName,
        int index,
        vector<HitPtr> &hitVec,
        FlatBufferBuilder &fbb,
        FBResultFormatter &formatter) = delete;
template <typename T>
void checkColumnValue(const Column *testSummaryTableColumn) = delete;
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(int8_t, Int8);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(int16_t, Int16);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(int32_t, Int32);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(int64_t, Int64);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint8_t, UInt8);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint16_t, UInt16);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint32_t, UInt32);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint64_t, UInt64);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(double, Double)
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(float, Float);


template <FieldType ft>
void testCreateSummaryTableColumnBySingleNumericType(
        autil::mem_pool::Pool &_pool) {
    typedef typename FieldTypeTraits<ft>::AttrItemType T;
    {
        FBResultFormatter formatter(&_pool);
        FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
        FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
        string clusterName = "clustername";
        HitPtr hitPtr1(new Hit(123, 1, clusterName));
        HitPtr hitPtr2(new Hit(456, 2, clusterName));
        HitPtr hitPtr3(new Hit(789, 3, clusterName));

        config::HitSummarySchema hitSummarySchema;
        hitSummarySchema.declareSummaryField("field0", ft);

        SummaryHit *summaryHit1 = new SummaryHit(&hitSummarySchema, &_pool);
        summaryHit1->setFieldValue(0, "11", 2);
        hitPtr1->setSummaryHit(summaryHit1);
        SummaryHit *summaryHit2 = new SummaryHit(&hitSummarySchema, &_pool);
        summaryHit2->setFieldValue(0, "12", 2);
        hitPtr2->setSummaryHit(summaryHit2);

        SummaryHit *summaryHit3 = new SummaryHit(&hitSummarySchema, &_pool);
        hitPtr3->setSummaryHit(summaryHit3);

        vector<HitPtr> hitVec;
        hitVec.push_back(hitPtr1);
        hitVec.push_back(hitPtr2);
        hitVec.push_back(hitPtr3);

        {
            fbb.Reset();
            Offset<Column> summaryTableColumn =
                createSummaryTableColumn<T>("field0", 0, hitVec, fbb, formatter);
            fbb.Finish(summaryTableColumn);
            char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());

            const Column *testSummaryTableColumn = GetRoot<Column>(data);
            const flatbuffers::String *testFieldName = testSummaryTableColumn->name();
            ASSERT_TRUE(testFieldName != nullptr);
            ASSERT_EQ(string("field0"), testFieldName->str());
            checkColumnValue<T>(testSummaryTableColumn);
        }
    }
}

#define DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft)               \
    TEST_F(                                                             \
            FBResultFormatterTest,                                      \
            testCreateSummaryTableColumnBySingleNumericType_##ft) {      \
        testCreateSummaryTableColumnBySingleNumericType<ft>(_pool);     \
    }
DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft_int8);
DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft_int16);
DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft_int32);
DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft_int64);
DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft_uint8);
DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft_uint16);
DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft_uint32);
DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft_uint64);
DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft_double);
DECLARE_TEST_CREATE_SUMMARY_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(ft_float);


TEST_F(FBResultFormatterTest, testCreateSummaryTableColumnByString) {
    FBResultFormatter formatter(&_pool);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    string clusterName = "clustername";
    HitPtr hitPtr1(new Hit(123, 1, clusterName));
    HitPtr hitPtr2(new Hit(456, 2, clusterName));

    config::HitSummarySchema hitSummarySchema;
    hitSummarySchema.declareSummaryField("field1");
    hitSummarySchema.declareSummaryField("field2");
    hitSummarySchema.declareSummaryField("field3");

    SummaryHit *summaryHit1 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit1->setFieldValue(0, "value10", 7);
    summaryHit1->setFieldValue(1, "value11", 7);
    hitPtr1->setSummaryHit(summaryHit1);

    SummaryHit *summaryHit2 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit2->setFieldValue(0, "value20", 7);
    summaryHit2->setFieldValue(2, "value22", 7);
    hitPtr2->setSummaryHit(summaryHit2);

    vector<HitPtr> hitVec;
    hitVec.push_back(hitPtr1);
    hitVec.push_back(hitPtr2);

    {
        fbb.Reset();
        Offset<Column> summaryTableColumn
            = formatter.CreateSummaryTableColumnByString("field1", 0, hitVec, fbb);
        fbb.Finish(summaryTableColumn);
        char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());

        const Column *testSummaryTableColumn = GetRoot<Column>(data);
        const flatbuffers::String *testFieldName = testSummaryTableColumn->name();
        ASSERT_TRUE(testFieldName != nullptr);
        ASSERT_EQ(string("field1"), testFieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, testSummaryTableColumn->value_type());

        const StringColumn *stringColumn = testSummaryTableColumn->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value10"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value20"), stringColumnValue->Get(1)->str());
    }

    {
        fbb.Reset();
        Offset<Column> summaryTableColumn
            = formatter.CreateSummaryTableColumnByString("field2", 1, hitVec, fbb);
        fbb.Finish(summaryTableColumn);
        char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());

        const Column *testSummaryTableColumn = GetRoot<Column>(data);
        const flatbuffers::String *testFieldName = testSummaryTableColumn->name();
        ASSERT_TRUE(testFieldName != nullptr);
        ASSERT_EQ(string("field2"), testFieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, testSummaryTableColumn->value_type());

        const StringColumn *stringColumn = testSummaryTableColumn->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value11"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string(""), stringColumnValue->Get(1)->str());
    }
    {
        fbb.Reset();
        Offset<Column> summaryTableColumn
            = formatter.CreateSummaryTableColumnByString("field3", 2, hitVec, fbb);
        fbb.Finish(summaryTableColumn);
        char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());

        const Column *testSummaryTableColumn = GetRoot<Column>(data);
        const flatbuffers::String *testFieldName = testSummaryTableColumn->name();
        ASSERT_TRUE(testFieldName != nullptr);
        ASSERT_EQ(string("field3"), testFieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, testSummaryTableColumn->value_type());

        const StringColumn *stringColumn = testSummaryTableColumn->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string(""), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value22"), stringColumnValue->Get(1)->str());
    }
}

TEST_F(FBResultFormatterTest, testCreateSummaryTable) {
    FBResultFormatter formatter(&_pool);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);

    string clusterName = "clustername";
    HitPtr hitPtr1(new Hit(123, 1, clusterName));
    HitPtr hitPtr2(new Hit(456, 2, clusterName));

    config::HitSummarySchema hitSummarySchema;
    hitSummarySchema.declareSummaryField("field1");
    hitSummarySchema.declareSummaryField("field2");
    hitSummarySchema.declareSummaryField("field3");

    SummaryHit *summaryHit1 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit1->setFieldValue(0, "value10", 7);
    summaryHit1->setFieldValue(1, "value11", 7);
    summaryHit1->setFieldValue(2, "", 0);
    hitPtr1->setSummaryHit(summaryHit1);

    SummaryHit *summaryHit2 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit2->setFieldValue(0, "value20", 7);
    summaryHit2->setFieldValue(1, "", 0);
    summaryHit2->setFieldValue(2, "value22", 7);
    hitPtr2->setSummaryHit(summaryHit2);

    shared_ptr<Hits> hitsPtr(new Hits());
    hitsPtr->addHit(hitPtr1);
    hitsPtr->addHit(hitPtr2);
    Offset<TwoDimTable> summaryTable = formatter.CreateSummaryTable(
            hitsPtr.get(), fbb);
    fbb.Finish(summaryTable);

    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());

    const TwoDimTable *testSummaryTable = GetRoot<TwoDimTable>(data);
    ASSERT_TRUE(testSummaryTable != nullptr);

    const Vector<Offset<Column> > *columns = testSummaryTable->columns();
    ASSERT_TRUE(columns != nullptr);

    {
        const Column *column1 = columns->Get(0);
        ASSERT_TRUE(column1 != nullptr);
        const flatbuffers::String *fieldName = column1->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field1"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column1->value_type());

        const StringColumn *stringColumn = column1->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value10"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value20"), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column2 = columns->Get(1);
        ASSERT_TRUE(column2 != nullptr);
        const flatbuffers::String *fieldName = column2->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field2"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column2->value_type());

        const StringColumn *stringColumn = column2->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value11"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string(""), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column3 = columns->Get(2);
        ASSERT_TRUE(column3 != nullptr);
        const flatbuffers::String *fieldName = column3->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field3"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column3->value_type());

        const StringColumn *stringColumn = column3->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string(""), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value22"), stringColumnValue->Get(1)->str());
    }
}

TEST_F(FBResultFormatterTest, testCreateSummaryResult) {
    FBResultFormatter formatter(&_pool);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);


    ResultPtr resultPtr(new Result);
    TracerPtr tracer(new Tracer());
    tracer->trace("test trace info");
    resultPtr->setTracer(tracer);

    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setHostInfo("12345", "test");
    errorResult.setErrorMsg("error message");

    MultiErrorResultPtr errorResults(new MultiErrorResult);
    errorResults->addErrorResult(errorResult);

    resultPtr->addErrorResult(errorResults);
    resultPtr->setTotalTime(123456);
    string clusterName = "clustername";
    HitPtr hitPtr1(new Hit(123, 1, clusterName));
    HitPtr hitPtr2(new Hit(456, 2, clusterName));

    config::HitSummarySchema hitSummarySchema;
    hitSummarySchema.declareSummaryField("field1");
    hitSummarySchema.declareSummaryField("field2");
    hitSummarySchema.declareSummaryField("field3");

    SummaryHit *summaryHit1 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit1->setFieldValue(0, "value10", 7);
    summaryHit1->setFieldValue(1, "value11", 7);
    summaryHit1->setFieldValue(2, "", 0);
    hitPtr1->setSummaryHit(summaryHit1);

    SummaryHit *summaryHit2 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit2->setFieldValue(0, "value20", 7);
    summaryHit2->setFieldValue(1, "", 0);
    summaryHit2->setFieldValue(2, "value22", 7);
    hitPtr2->setSummaryHit(summaryHit2);

    Hits *hits = new Hits();
    hits->addHit(hitPtr1);
    hits->addHit(hitPtr2);

    resultPtr->setHits(hits);

    Offset<SummaryResult> summaryResult = formatter.CreateSummaryResult(
            resultPtr, fbb);
    fbb.Finish(summaryResult);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const SummaryResult *testSummaryResult = GetRoot<SummaryResult>(data);
    ASSERT_TRUE(testSummaryResult != nullptr);

    ASSERT_EQ(123456, testSummaryResult->totalTime());

    const flatbuffers::String *testTracer = testSummaryResult->tracer();
    ASSERT_TRUE(testTracer != nullptr);
    ASSERT_EQ(string("test trace info\n"), testTracer->str());

    const Vector<Offset<FBErrorResult> > *testErrorResults =
        testSummaryResult->errorResults();
    ASSERT_TRUE(testErrorResults != nullptr);
    ASSERT_EQ((size_t)1, testErrorResults->size());
    const FBErrorResult *fbErrorResult = testErrorResults->Get(0);
    ASSERT_TRUE(fbErrorResult != nullptr);
    ASSERT_EQ("12345", fbErrorResult->partitionId()->str());
    ASSERT_EQ("test", fbErrorResult->hostName()->str());
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, fbErrorResult->errorDescription()->str());

    const TwoDimTable *summaryTable = testSummaryResult->summaryTable();
    ASSERT_TRUE(summaryTable != nullptr);
    const Vector<Offset<Column> > *columns = summaryTable->columns();
    ASSERT_TRUE(columns != nullptr);

    {
        const Column *column1 = columns->Get(0);
        ASSERT_TRUE(column1 != nullptr);
        const flatbuffers::String *fieldName = column1->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field1"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column1->value_type());

        const StringColumn *stringColumn = column1->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value10"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value20"), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column2 = columns->Get(1);
        ASSERT_TRUE(column2 != nullptr);
        const flatbuffers::String *fieldName = column2->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field2"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column2->value_type());

        const StringColumn *stringColumn = column2->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value11"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string(""), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column3 = columns->Get(2);
        ASSERT_TRUE(column3 != nullptr);
        const flatbuffers::String *fieldName = column3->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field3"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column3->value_type());

        const StringColumn *stringColumn = column3->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string(""), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value22"), stringColumnValue->Get(1)->str());
    }
}

TEST_F(FBResultFormatterTest, testFormatPerf) {
    FBResultFormatter formatter(&_pool);
    stringstream ss;
    ResultPtr resultPtr(new Result);
    TracerPtr tracer(new Tracer());
    tracer->trace("test trace info");
    resultPtr->setTracer(tracer);

    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setHostInfo("12345", "test");
    errorResult.setErrorMsg("error message");

    MultiErrorResultPtr errorResults(new MultiErrorResult);
    errorResults->addErrorResult(errorResult);

    resultPtr->addErrorResult(errorResults);
    resultPtr->setTotalTime(123456);

    config::HitSummarySchema hitSummarySchema;
    size_t fieldCount = 20;
    for (size_t i = 0; i < fieldCount; i++) {
        string fieldName = string("field") + autil::StringUtil::toString(i);
        hitSummarySchema.declareSummaryField(fieldName);
    }


    string clusterName = "clustername";
    size_t hitCount = 50;
    Hits *hits = new Hits();
    for (size_t i = 0; i < hitCount; i++) {
        HitPtr hitPtr(new Hit(123, 1, clusterName));
        hits->addHit(hitPtr);
        SummaryHit *summaryHit = new SummaryHit(&hitSummarySchema, &_pool);
        for (size_t j = 0; j < fieldCount; j++) {
            summaryHit->setFieldValue(j, "value10", 7);
        }
        hitPtr->setSummaryHit(summaryHit);
    }

    hits->setTotalHits(100);
    resultPtr->setHits(hits);
    int64_t beginTime = TimeUtility::currentTime();
    for (size_t i = 0; i < 1000; i++) {
        formatter.format(resultPtr);
    }

    int64_t endTime = TimeUtility::currentTime();
    cout << "fb formatter used time:" << endTime - beginTime<<endl;


    PBResultFormatter pbResultFormatter;
    beginTime = TimeUtility::currentTime();
    for (size_t i = 0; i < 1000; i++) {
        pbResultFormatter.format(resultPtr, ss);
    }
    endTime = TimeUtility::currentTime();
    cout << "pb formatter used time:" << endTime - beginTime<<endl;
}

TEST_F(FBResultFormatterTest, testCreateSummaryResultFirstEmptySummaryHit) {
    FBResultFormatter formatter(&_pool);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);


    ResultPtr resultPtr(new Result);
    TracerPtr tracer(new Tracer());
    tracer->trace("test trace info");
    resultPtr->setTracer(tracer);

    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setHostInfo("12345", "test");
    errorResult.setErrorMsg("error message");

    MultiErrorResultPtr errorResults(new MultiErrorResult);
    errorResults->addErrorResult(errorResult);

    resultPtr->addErrorResult(errorResults);
    resultPtr->setTotalTime(123456);
    string clusterName = "clustername";
    HitPtr hitPtr1(new Hit(123, 1, clusterName));
    HitPtr hitPtr2(new Hit(456, 2, clusterName));

    config::HitSummarySchema hitSummarySchema;
    hitSummarySchema.declareSummaryField("field1");
    hitSummarySchema.declareSummaryField("field2");
    hitSummarySchema.declareSummaryField("field3");

    SummaryHit *summaryHit2 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit2->setFieldValue(0, "value20", 7);
    summaryHit2->setFieldValue(1, "", 0);
    summaryHit2->setFieldValue(2, "value22", 7);
    hitPtr2->setSummaryHit(summaryHit2);

    Hits *hits = new Hits();
    hits->addHit(hitPtr1);
    hits->addHit(hitPtr2);

    resultPtr->setHits(hits);

    Offset<SummaryResult> summaryResult = formatter.CreateSummaryResult(
            resultPtr, fbb);
    fbb.Finish(summaryResult);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const SummaryResult *testSummaryResult = GetRoot<SummaryResult>(data);
    ASSERT_TRUE(testSummaryResult != nullptr);

    ASSERT_EQ(123456, testSummaryResult->totalTime());

    const flatbuffers::String *testTracer = testSummaryResult->tracer();
    ASSERT_TRUE(testTracer != nullptr);
    ASSERT_EQ(string("test trace info\n"), testTracer->str());

    const Vector<Offset<FBErrorResult> > *testErrorResults =
        testSummaryResult->errorResults();
    ASSERT_TRUE(testErrorResults != nullptr);
    ASSERT_EQ((size_t)1, testErrorResults->size());
    const FBErrorResult *fbErrorResult = testErrorResults->Get(0);
    ASSERT_TRUE(fbErrorResult != nullptr);
    ASSERT_EQ("12345", fbErrorResult->partitionId()->str());
    ASSERT_EQ("test", fbErrorResult->hostName()->str());
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, fbErrorResult->errorDescription()->str());

    const TwoDimTable *summaryTable = testSummaryResult->summaryTable();
    ASSERT_TRUE(summaryTable != nullptr);
    const Vector<Offset<Column> > *columns = summaryTable->columns();
    ASSERT_TRUE(columns != nullptr);

    {
        const Column *column1 = columns->Get(0);
        ASSERT_TRUE(column1 != nullptr);
        const flatbuffers::String *fieldName = column1->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field1"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column1->value_type());

        const StringColumn *stringColumn = column1->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string(""), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value20"), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column2 = columns->Get(1);
        ASSERT_TRUE(column2 != nullptr);
        const flatbuffers::String *fieldName = column2->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field2"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column2->value_type());

        const StringColumn *stringColumn = column2->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string(""), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string(""), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column3 = columns->Get(2);
        ASSERT_TRUE(column3 != nullptr);
        const flatbuffers::String *fieldName = column3->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field3"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column3->value_type());

        const StringColumn *stringColumn = column3->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string(""), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value22"), stringColumnValue->Get(1)->str());
    }
}

TEST_F(FBResultFormatterTest, testCreateSummaryResultSecondEmptySummaryHit) {
    FBResultFormatter formatter(&_pool);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);


    ResultPtr resultPtr(new Result);
    TracerPtr tracer(new Tracer());
    tracer->trace("test trace info");
    resultPtr->setTracer(tracer);

    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setHostInfo("12345", "test");
    errorResult.setErrorMsg("error message");

    MultiErrorResultPtr errorResults(new MultiErrorResult);
    errorResults->addErrorResult(errorResult);

    resultPtr->addErrorResult(errorResults);
    resultPtr->setTotalTime(123456);
    string clusterName = "clustername";
    HitPtr hitPtr1(new Hit(123, 1, clusterName));
    HitPtr hitPtr2(new Hit(456, 2, clusterName));

    config::HitSummarySchema hitSummarySchema;
    hitSummarySchema.declareSummaryField("field1");
    hitSummarySchema.declareSummaryField("field2");
    hitSummarySchema.declareSummaryField("field3");

    SummaryHit *summaryHit1 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit1->setFieldValue(0, "value10", 7);
    summaryHit1->setFieldValue(1, "value11", 7);
    summaryHit1->setFieldValue(2, "", 0);
    hitPtr1->setSummaryHit(summaryHit1);

    Hits *hits = new Hits();
    hits->addHit(hitPtr1);
    hits->addHit(hitPtr2);

    resultPtr->setHits(hits);

    Offset<SummaryResult> summaryResult = formatter.CreateSummaryResult(
            resultPtr, fbb);
    fbb.Finish(summaryResult);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const SummaryResult *testSummaryResult = GetRoot<SummaryResult>(data);
    ASSERT_TRUE(testSummaryResult != nullptr);

    ASSERT_EQ(123456, testSummaryResult->totalTime());

    const flatbuffers::String *testTracer = testSummaryResult->tracer();
    ASSERT_TRUE(testTracer != nullptr);
    ASSERT_EQ(string("test trace info\n"), testTracer->str());

    const Vector<Offset<FBErrorResult> > *testErrorResults =
        testSummaryResult->errorResults();
    ASSERT_TRUE(testErrorResults != nullptr);
    ASSERT_EQ((size_t)1, testErrorResults->size());
    const FBErrorResult *fbErrorResult = testErrorResults->Get(0);
    ASSERT_TRUE(fbErrorResult != nullptr);
    ASSERT_EQ("12345", fbErrorResult->partitionId()->str());
    ASSERT_EQ("test", fbErrorResult->hostName()->str());
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, fbErrorResult->errorDescription()->str());

    const TwoDimTable *summaryTable = testSummaryResult->summaryTable();
    ASSERT_TRUE(summaryTable != nullptr);
    const Vector<Offset<Column> > *columns = summaryTable->columns();
    ASSERT_TRUE(columns != nullptr);

    {
        const Column *column1 = columns->Get(0);
        ASSERT_TRUE(column1 != nullptr);
        const flatbuffers::String *fieldName = column1->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field1"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column1->value_type());

        const StringColumn *stringColumn = column1->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value10"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string(""), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column2 = columns->Get(1);
        ASSERT_TRUE(column2 != nullptr);
        const flatbuffers::String *fieldName = column2->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field2"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column2->value_type());

        const StringColumn *stringColumn = column2->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value11"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string(""), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column3 = columns->Get(2);
        ASSERT_TRUE(column3 != nullptr);
        const flatbuffers::String *fieldName = column3->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field3"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column3->value_type());

        const StringColumn *stringColumn = column3->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string(""), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string(""), stringColumnValue->Get(1)->str());
    }
}

TEST_F(FBResultFormatterTest, testCreateSummaryResultLackColumn) {
    FBResultFormatter formatter(&_pool);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);


    ResultPtr resultPtr(new Result);
    TracerPtr tracer(new Tracer());
    tracer->trace("test trace info");
    resultPtr->setTracer(tracer);

    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setHostInfo("12345", "test");
    errorResult.setErrorMsg("error message");

    MultiErrorResultPtr errorResults(new MultiErrorResult);
    errorResults->addErrorResult(errorResult);

    resultPtr->addErrorResult(errorResults);
    resultPtr->setTotalTime(123456);
    string clusterName = "clustername";
    HitPtr hitPtr1(new Hit(123, 1, clusterName));
    HitPtr hitPtr2(new Hit(456, 2, clusterName));

    config::HitSummarySchema hitSummarySchema;
    hitSummarySchema.declareSummaryField("field1");
    hitSummarySchema.declareSummaryField("field2");
    hitSummarySchema.declareSummaryField("field3");

    SummaryHit *summaryHit1 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit1->setFieldValue(1, "value11", 7);
    summaryHit1->setFieldValue(2, "value12", 7);
    hitPtr1->setSummaryHit(summaryHit1);


    SummaryHit *summaryHit2 = new SummaryHit(&hitSummarySchema, &_pool);
    summaryHit2->setFieldValue(0, "value20", 7);
    summaryHit2->setFieldValue(1, "value21", 7);
    hitPtr2->setSummaryHit(summaryHit2);

    Hits *hits = new Hits();
    hits->addHit(hitPtr1);
    hits->addHit(hitPtr2);

    resultPtr->setHits(hits);

    Offset<SummaryResult> summaryResult = formatter.CreateSummaryResult(
            resultPtr, fbb);
    fbb.Finish(summaryResult);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const SummaryResult *testSummaryResult = GetRoot<SummaryResult>(data);
    ASSERT_TRUE(testSummaryResult != nullptr);

    ASSERT_EQ(123456, testSummaryResult->totalTime());

    const flatbuffers::String *testTracer = testSummaryResult->tracer();
    ASSERT_TRUE(testTracer != nullptr);
    ASSERT_EQ(string("test trace info\n"), testTracer->str());

    const Vector<Offset<FBErrorResult> > *testErrorResults =
        testSummaryResult->errorResults();
    ASSERT_TRUE(testErrorResults != nullptr);
    ASSERT_EQ((size_t)1, testErrorResults->size());
    const FBErrorResult *fbErrorResult = testErrorResults->Get(0);
    ASSERT_TRUE(fbErrorResult != nullptr);
    ASSERT_EQ("12345", fbErrorResult->partitionId()->str());
    ASSERT_EQ("test", fbErrorResult->hostName()->str());
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, fbErrorResult->errorDescription()->str());

    const TwoDimTable *summaryTable = testSummaryResult->summaryTable();
    ASSERT_TRUE(summaryTable != nullptr);
    const Vector<Offset<Column> > *columns = summaryTable->columns();
    ASSERT_TRUE(columns != nullptr);

    {
        const Column *column1 = columns->Get(0);
        ASSERT_TRUE(column1 != nullptr);
        const flatbuffers::String *fieldName = column1->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field2"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column1->value_type());

        const StringColumn *stringColumn = column1->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value11"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string("value21"), stringColumnValue->Get(1)->str());
    }

    {
        const Column *column2 = columns->Get(1);
        ASSERT_TRUE(column2 != nullptr);
        const flatbuffers::String *fieldName = column2->name();
        ASSERT_TRUE(fieldName != nullptr);
        ASSERT_EQ(string("field3"), fieldName->str());
        ASSERT_EQ(ColumnType_StringColumn, column2->value_type());

        const StringColumn *stringColumn = column2->value_as_StringColumn();
        ASSERT_TRUE(stringColumn != nullptr);
        const Vector<Offset<flatbuffers::String> > *stringColumnValue =
            stringColumn->value();
        ASSERT_EQ(2, stringColumnValue->size());
        ASSERT_EQ(string("value12"), stringColumnValue->Get(0)->str());
        ASSERT_EQ(string(""), stringColumnValue->Get(1)->str());
    }
}

TEST_F(FBResultFormatterTest, testCreateSummaryResultEmptyHits) {
    FBResultFormatter formatter(&_pool);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);


    ResultPtr resultPtr(new Result);
    TracerPtr tracer(new Tracer());
    tracer->trace("test trace info");
    resultPtr->setTracer(tracer);

    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setHostInfo("12345", "test");
    errorResult.setErrorMsg("error message");

    MultiErrorResultPtr errorResults(new MultiErrorResult);
    errorResults->addErrorResult(errorResult);

    resultPtr->addErrorResult(errorResults);
    resultPtr->setTotalTime(123456);
    string clusterName = "clustername";
    Hits *hits = new Hits();

    resultPtr->setHits(hits);

    Offset<SummaryResult> summaryResult = formatter.CreateSummaryResult(
            resultPtr, fbb);
    fbb.Finish(summaryResult);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const SummaryResult *testSummaryResult = GetRoot<SummaryResult>(data);
    ASSERT_TRUE(testSummaryResult != nullptr);

    ASSERT_EQ(123456, testSummaryResult->totalTime());

    const flatbuffers::String *testTracer = testSummaryResult->tracer();
    ASSERT_TRUE(testTracer != nullptr);
    ASSERT_EQ(string("test trace info\n"), testTracer->str());

    const Vector<Offset<FBErrorResult> > *testErrorResults =
        testSummaryResult->errorResults();
    ASSERT_TRUE(testErrorResults != nullptr);
    ASSERT_EQ((size_t)1, testErrorResults->size());
    const FBErrorResult *fbErrorResult = testErrorResults->Get(0);
    ASSERT_TRUE(fbErrorResult != nullptr);
    ASSERT_EQ("12345", fbErrorResult->partitionId()->str());
    ASSERT_EQ("test", fbErrorResult->hostName()->str());
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, fbErrorResult->errorDescription()->str());

    const TwoDimTable *summaryTable = testSummaryResult->summaryTable();
    ASSERT_TRUE(summaryTable != nullptr);
    const Vector<Offset<Column> > *columns = summaryTable->columns();
    ASSERT_TRUE(columns != nullptr);
    ASSERT_TRUE(columns->size() == 0);
}

TEST_F(FBResultFormatterTest, testCreateSummaryResultNullHits) {
    FBResultFormatter formatter(&_pool);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);

    ResultPtr resultPtr(new Result);
    TracerPtr tracer(new Tracer());
    tracer->trace("test trace info");
    resultPtr->setTracer(tracer);

    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setHostInfo("12345", "test");
    errorResult.setErrorMsg("error message");

    MultiErrorResultPtr errorResults(new MultiErrorResult);
    errorResults->addErrorResult(errorResult);

    resultPtr->addErrorResult(errorResults);
    resultPtr->setTotalTime(123456);
    string clusterName = "clustername";

    Offset<SummaryResult> summaryResult = formatter.CreateSummaryResult(
            resultPtr, fbb);
    fbb.Finish(summaryResult);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const SummaryResult *testSummaryResult = GetRoot<SummaryResult>(data);
    ASSERT_TRUE(testSummaryResult != nullptr);

    ASSERT_EQ(123456, testSummaryResult->totalTime());

    const flatbuffers::String *testTracer = testSummaryResult->tracer();
    ASSERT_TRUE(testTracer != nullptr);
    ASSERT_EQ(string("test trace info\n"), testTracer->str());

    const Vector<Offset<FBErrorResult> > *testErrorResults =
        testSummaryResult->errorResults();
    ASSERT_TRUE(testErrorResults != nullptr);
    ASSERT_EQ((size_t)1, testErrorResults->size());
    const FBErrorResult *fbErrorResult = testErrorResults->Get(0);
    ASSERT_TRUE(fbErrorResult != nullptr);
    ASSERT_EQ("12345", fbErrorResult->partitionId()->str());
    ASSERT_EQ("test", fbErrorResult->hostName()->str());
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, fbErrorResult->errorDescription()->str());

    const TwoDimTable *summaryTable = testSummaryResult->summaryTable();
    ASSERT_TRUE(summaryTable != nullptr);
    const Vector<Offset<Column> > *columns = summaryTable->columns();
    ASSERT_TRUE(columns != nullptr);
    ASSERT_TRUE(columns->size() == 0);
}

END_HA3_NAMESPACE(common);
