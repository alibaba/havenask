#include <ha3/common/FBResultFormatter.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/Hits.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;
using namespace isearch::fbs;
using namespace flatbuffers;

USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, FBResultFormatter);

static const uint32_t FB_BUILDER_INIT_SIZE = 1024 * 1024;

FBResultFormatter::FBResultFormatter(Pool *pool)
    : _pool(pool)
{

}

FBResultFormatter::~FBResultFormatter() {
}

string FBResultFormatter::format(const ResultPtr &resultPtr)
{
    FlatBufferSimpleAllocator flatbufferAllocator(_pool);
    FlatBufferBuilder fbb(FB_BUILDER_INIT_SIZE, &flatbufferAllocator);

    Offset<SummaryResult> summaryResult = CreateSummaryResult(resultPtr, fbb);
    fbb.Finish(summaryResult);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    return string(data, fbb.GetSize());
}

Offset<SummaryResult> FBResultFormatter::CreateSummaryResult(
        const ResultPtr &resultPtr,
        FlatBufferBuilder &fbb) {
    uint32_t totalTime = resultPtr->getTotalTime();
    uint32_t totalHits = resultPtr->getTotalHits();
    MultiErrorResultPtr &multiErrorResult = resultPtr->getMultiErrorResult();

    Tracer *tracer = resultPtr->getTracer();

    const Hits *hits = resultPtr->getHits();

    return isearch::fbs::CreateSummaryResult(fbb, totalTime, totalHits,
            CreateErrorResults(multiErrorResult, fbb),
            CreateTracer(tracer, fbb),
            CreateSummaryTable(hits, fbb));
}

Offset<flatbuffers::String> FBResultFormatter::CreateTracer(
            const Tracer *tracer,
            FlatBufferBuilder &fbb) {
    Offset<flatbuffers::String> fbTracer;

    if (tracer) {
        fbTracer = fbb.CreateString(tracer->getTraceInfo());
    }
    return fbTracer;
}

Offset<Vector<Offset<FBErrorResult> > >  FBResultFormatter::CreateErrorResults(
        const MultiErrorResultPtr &multiErrorResult,
        FlatBufferBuilder &fbb) {
    const ErrorResults &errorResults = multiErrorResult->getErrorResults();
    vector<Offset<FBErrorResult> > fbErrorResults;
    for (ErrorResults::const_iterator it = errorResults.begin();
         it != errorResults.end(); ++it)
    {
        fbErrorResults.push_back(CreateFBErrorResult(fbb,
                        fbb.CreateString(it->getPartitionID()),
                        fbb.CreateString(it->getHostName()),
                        it->getErrorCode(),
                        fbb.CreateString(it->getErrorDescription())));
    }
    return fbb.CreateVector(fbErrorResults);
}

SummaryHit *FBResultFormatter::findFirstNonEmptySummaryHit(
        const vector<HitPtr> &hitVec) {
    for (auto &hit: hitVec) {
        auto *summaryHit = hit->getSummaryHit();
        if (summaryHit) {
            return summaryHit;
        }
    }
    return nullptr;
}

Offset<TwoDimTable> FBResultFormatter::CreateSummaryTable(
        const Hits *hits, FlatBufferBuilder &fbb) {
    vector<Offset<Column> > summaryTableColumns;
    size_t rowCount = 0u;
    if (hits) {
        const vector<HitPtr> &hitVec = hits->getHitVec();
        rowCount = hitVec.size();
        if (rowCount > 0) {
            auto *summaryHit = findFirstNonEmptySummaryHit(hitVec);
            if (summaryHit) {
                const HitSummarySchema *hitSummarySchema =
                    summaryHit->getHitSummarySchema();
                size_t fieldCount = summaryHit->getFieldCount();
                for (size_t fieldIdx = 0; fieldIdx < fieldCount; fieldIdx++) {
                    const ConstString *str = summaryHit->getFieldValue(fieldIdx);
                    if (!str) {
                        continue;
                    }
                    const SummaryFieldInfo *fieldInfo =
                        hitSummarySchema->getSummaryFieldInfo(fieldIdx);
                    const string &fieldName =
                        fieldInfo->fieldName;
                    FieldType fieldType = fieldInfo->fieldType;
                    bool isMultiValue = fieldInfo->isMultiValue;
                    if (isMultiValue || fieldType == ft_string) {
                        summaryTableColumns.push_back(
                                CreateSummaryTableColumnByString(
                                        fieldName, fieldIdx, hitVec, fbb));
                    } else {
#define NUMERIC_SWITCH_CASE_HELPER(MY_MACRO)       \
                        MY_MACRO(UInt8, ft_uint8);    \
                        MY_MACRO(Int8, ft_int8);      \
                        MY_MACRO(UInt16, ft_uint16);  \
                        MY_MACRO(Int16, ft_int16);    \
                        MY_MACRO(UInt32, ft_uint32);  \
                        MY_MACRO(Int32, ft_int32);    \
                        MY_MACRO(UInt64, ft_uint64);  \
                        MY_MACRO(Int64, ft_int64);    \
                        MY_MACRO(Float, ft_float);      \
                        MY_MACRO(Double, ft_double);

#define NUMERIC_SWITCH_CASE(fbType, fieldType)  \
                        case fieldType:                         \
                            summaryTableColumns.push_back(              \
                                    CreateSummaryTableColumnBy##fbType( \
                                            fieldName, fieldIdx, hitVec, fbb)); \
                            break;

                        switch(fieldType) {
                            NUMERIC_SWITCH_CASE_HELPER(NUMERIC_SWITCH_CASE);
                            default:
                                summaryTableColumns.push_back(
                                        CreateSummaryTableColumnByString(
                                                fieldName, fieldIdx, hitVec, fbb));
                        }
                    }
                }
            }
        }
    }
#undef NUMERIC_SWITCH_CASE
#undef NUMERIC_SWITCH_CASE_HELPER

    return CreateTwoDimTable(
            fbb, (uint32_t) rowCount, fbb.CreateVector(summaryTableColumns));
}

Offset<Column> FBResultFormatter::CreateSummaryTableColumnByString(
        const string &fieldName, size_t summaryFieldIdx,
        const vector<HitPtr> &hitVec, FlatBufferBuilder &fbb) {

    vector<Offset<flatbuffers::String> > columnValue;

    for (auto &hit : hitVec) {
        SummaryHit *summaryHit = hit->getSummaryHit();
        if (!summaryHit) {
            columnValue.push_back(fbb.CreateString(""));
            continue;
        }
        const ConstString *str = summaryHit->getFieldValue(summaryFieldIdx);
        if (!str) {
            columnValue.push_back(fbb.CreateString(""));
            continue;
        }
        columnValue.push_back(fbb.CreateString(str->data(), str->size()));
    }
    Offset<StringColumn> fb_columnValue =
        CreateStringColumn(fbb, fbb.CreateVector(columnValue));
    return CreateColumnDirect(fbb, fieldName.c_str(), ColumnType_StringColumn, fb_columnValue.Union());
}


#define DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(fbType, fieldType)  \
    Offset<Column> FBResultFormatter::CreateSummaryTableColumnBy##fbType( \
            const string &fieldName, size_t summaryFieldIdx,            \
            const vector<HitPtr> &hitVec, FlatBufferBuilder &fbb) {     \
    vector<fieldType> columnValue;                                      \
    for (auto &hit : hitVec) {                                           \
    SummaryHit *summaryHit = hit->getSummaryHit();                      \
    if (!summaryHit) {                                                  \
        columnValue.push_back((fieldType)0);                            \
        continue;                                                       \
    }                                                                   \
    const ConstString *str = summaryHit->getFieldValue(summaryFieldIdx); \
    if (!str) {                                                         \
        columnValue.push_back((fieldType)0);                            \
        continue;                                                       \
    }                                                                   \
    columnValue.push_back(                                              \
            autil::StringUtil::fromString<fieldType>(string(str->data()))); \
    }                                                                   \
    Offset<fbType##Column> fb_columnValue =                             \
            Create##fbType##Column(fbb, fbb.CreateVector(columnValue)); \
    return CreateColumnDirect(fbb, fieldName.c_str(), \
                              ColumnType_##fbType##Column, fb_columnValue.Union()); \
    }

DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(UInt8, uint8_t);
DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(Int8, int8_t);
DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(UInt16, uint16_t);
DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(Int16, int16_t);
DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(UInt32, uint32_t);
DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(Int32, int32_t);
DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(UInt64, uint64_t);
DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(Int64, int64_t);
DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(Float, float);
DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE(Double, double);

#undef DECLARE_CREATE_SUMMARY_TABLE_COLUMN_BY_TYPE

END_HA3_NAMESPACE(common);
