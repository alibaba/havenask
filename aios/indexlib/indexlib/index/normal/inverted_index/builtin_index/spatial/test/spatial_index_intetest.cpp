#include <autil/TimeUtility.h>
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/inverted_index/builtin_index/spatial/test/spatial_index_intetest.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/config/impl/spatial_index_config_impl.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h"
#include "indexlib/partition/offline_partition.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SpatialIndexInteTest);

SpatialIndexInteTest::SpatialIndexInteTest()
{
}

SpatialIndexInteTest::~SpatialIndexInteTest()
{
}

void SpatialIndexInteTest::CaseSetUp()
{
    string field = "pk:string:pk;coordinate:location;polygon:polygon";
    string index = "pk:primarykey64:pk;spatial_index:spatial:coordinate;"
                   "polygon_index:spatial:polygon";
    mSchema = SchemaMaker::MakeSchema(field, index, "coordinate;polygon", "");
    ASSERT_TRUE(mSchema);
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);
}

void SpatialIndexInteTest::CaseTearDown()
{
}

void SpatialIndexInteTest::TestSchemaMaker()
{
    string field = "line:line;polygon:polygon";
    string index = "line_index:spatial:line;polygon_index:spatial:polygon";
    string attribute = "line;polygon";
    string summary = "";

    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, summary);
}

void SpatialIndexInteTest::TestPolygonSearch()
{
    string docString = "cmd=add|pk=1|polygon=0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1,0.1 30.1";
    RewriteSpatialConfig(mSchema, 100000000000.0, 5.0, "polygon_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "polygon_index:polygon(0.0 40.0,40.0 40.0,40.0 0.0,0.0 0.0,0 40.0)",
                             "docid=0|polygon=0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1,0.1 30.1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "polygon_index:polygon(0.0 0.0,0.0 45.0,45.0 45.0,45.0 0.0,0.0 0.0)",
                             "docid=0"));

    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "polygon_index:polygon(0.0 17.0,0.0 21.0,11.0 21.0,11.0 17.0,0.0 17.0)",
                             "docid=0"));

    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "polygon_index:point(11.0 21.0)",
                             "docid=0"));

    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "polygon_index:point(29.0 21.0)",
                             "docid=0"));

    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "polygon_index:point(40.0 50.0)",
                             ""));

    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "polygon_index:polygon(0.0 17.0,0.0 21.0,11.0 21.0,11.0 17.0,0.0 17.0)",
                             "docid=0"));

    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "polygon_index:line(20.0 50.0,10.0 17.0,35.0 40.0,45.0 17.0)",
                             "docid=0"));

    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "polygon_index:line(20.0 50.0,40.0 41.0,45.0 50.0)",
                             ""));
}

void SpatialIndexInteTest::TestPolygonLeafSearch()
{
    string docString = "cmd=add|pk=1|polygon=97.39374 33.844938,97.780536 33.367,97.44659 32.979707,97.543289 32.563474,98.338709 32.354629,98.736419 31.653433,98.803438 30.924593,99.012729 29.760153,99.205265 28.209051,99.60915 28.858578,100.294491 28.295606,100.241654 27.818837,100.742579 27.929667,101.212768 26.932207,101.711534 26.154332,102.664043 26.355593,103.008461 26.456092,102.785162 27.329166,103.332692 27.685353,103.562512 28.057092,103.897149 28.281188,103.836277 28.736268,104.399484 28.402617,104.505306 28.002845,105.217396 28.112932,105.573442 27.74094,106.234863 27.865706,106.301901 28.083233,106.027804 28.11434,105.671028 28.362164,105.976039 28.717356,106.743778 28.470421,107.215539 28.732788,107.53931 29.017556,107.920922 29.198015,108.551181 28.82716,108.910256 28.293549,109.353465 28.451598,109.267453 29.031248,108.960774 29.511261,108.499818 29.902915,108.401176 30.288086,108.835253 30.517971,109.579636 30.670516,110.127608 30.935354,110.357649 31.405671,109.725599 31.639951,108.486723 32.241353,108.350683 31.908584,108.326608 31.479771,107.8092 30.888241,108.407429 32.166764,107.43213 32.46634,106.812644 32.874488,105.797651 32.764923,105.129252 32.658835,104.399544 32.975172,104.122581 33.500145,103.676483 33.724904,103.101871 33.69093,102.990346 34.293401,102.40724 34.048622,102.55514 33.450618,101.838075 33.150057,101.307246 33.313787,100.952198 32.444823,100.17967 32.868609,99.758704 32.774594,98.80927 33.338687,98.431125 34.117722,97.850617 34.140671,97.39374 33.844938";
    RewriteSpatialConfig(mSchema, 100000, 1000, "polygon_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "polygon_index:point(103.542207 30.212242)",
                             "docid=0"));
}

void SpatialIndexInteTest::TestLazyloadSpatialAttribute()
{
    string field = "pk:string:pk;coordinate:location;line:line;polygon:polygon";
    string index = "pk:primarykey64:pk";
    mSchema = SchemaMaker::MakeSchema(field, index, "coordinate;polygon;line", "");
    string docString = "cmd=add|pk=1|polygon="
                       "0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1,0.1 30.1|"
                       "line=0.1 30.1,30.1 30.1|coordinate=0.1 30.1";
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    partition::OfflinePartition offlinePartition;
    mOptions.SetIsOnline(false);
    offlinePartition.Open(GET_TEST_DATA_PATH(), "", mSchema, mOptions);
    partition::IndexPartitionReaderPtr indexReader = offlinePartition.GetReader();
    ASSERT_TRUE(indexReader);
    RawDocumentFieldExtractor extractor;
    vector<string> fieldNames;
    fieldNames.push_back("polygon");
    fieldNames.push_back("line");
    fieldNames.push_back("coordinate");
    extractor.Init(indexReader, fieldNames);
    InnerCheckIterator(extractor, 0, fieldNames,
                       "0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1,0.1 30.1"
                       "|0.1 30.1,30.1 30.1|0.1 30.1");
}

void SpatialIndexInteTest::InnerCheckIterator(
    RawDocumentFieldExtractor& extractor, docid_t docId,
    const vector<string>& fieldNames, const string& fieldValues)
{
    if (fieldValues.empty())
    {
        ASSERT_EQ(SS_DELETED, extractor.Seek(docId));
        return;
    }
    ASSERT_EQ(SS_OK, extractor.Seek(docId));

    auto iter = extractor.CreateIterator();
    vector<string> expectValues;
    StringUtil::fromString(fieldValues, expectValues, "|");

    for (size_t i = 0; i < expectValues.size(); ++i)
    {
        string name;
        string value;
        iter.Next(name, value);
        ASSERT_EQ(fieldNames[i], name);
        ASSERT_EQ(expectValues[i], value);
    }
    ASSERT_FALSE(iter.HasNext());
}


void SpatialIndexInteTest::TestLineSearch()
{
    string field = "pk:string:pk;coordinate:location;line:line";
    string index = "pk:primarykey64:pk;spatial_index:spatial:coordinate;"
                   "line_index:spatial:line";
    mSchema = SchemaMaker::MakeSchema(field, index, "coordinate;line", "");
    ASSERT_TRUE(mSchema);
    string docString = "cmd=add|pk=1|line=0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1";
    RewriteSpatialConfig(mSchema, 100000000000.0, 500000.0, "line_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "line_index:polygon(0.0 40.0,40.0 40.0,40.0 0.0,0.0 0.0,0 40.0)",
                             "docid=0|line=0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1"));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "line_index:point(30.1 30.1)",
                             "docid=0|line=0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1"));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "line_index:point(15.1 30.1)",
                             "docid=0|line=0.1 30.1,30.1 30.1,30.1 0.1,0.1 0.1"));
}

void SpatialIndexInteTest::TestAttributeInputIllegal()
{
    string docString = "cmd=add,pk=1,coordinate=116.3906 39.92324116.3906;"
        "cmd=add,pk=2,coordinate=116.3906 39.92324a 116.3906;"
       "cmd=add,pk=3,coordinate=116.3906 39.9232410000.0 116.3906";
    RewriteSpatialConfig(mSchema, 100000.0, 50.0, "spatial_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "spatial_index:point(116.3906 39.92324)",
                             "docid=0,coordinate=116.3906 39.92324;"
                             "docid=1,coordinate=116.3906 39.92324;"
                             "docid=2,coordinate=116.3906 39.92324"));
}

void SpatialIndexInteTest::TestSimpleProcess()
{
    string docString = "cmd=add,pk=1,coordinate=116.3906 39.92324116.3906 39.92325;"
                       "cmd=add,pk=2,coordinate=116.3906 39.92324;"
                       "cmd=add,pk=3,coordinate=116.3906 39.92324;"
                       "cmd=add,pk=4,coordinate=116.3906 39.92325;"
                       "cmd=add,pk=5,coordinate=116.3906 39.92335;"
                       "cmd=add,pk=6,coordinate=116.3906 39.92435;"
                       "cmd=add,pk=7,coordinate=-22.5 -0.1;"
                       "cmd=add,pk=8,coordinate=-44.9 -22.5;"
                       "cmd=add,pk=9,coordinate=-22.5 -44.9;"
                       "cmd=add,pk=10,coordinate=-0.1 -22.5;"
                       ;
    // RewriteSpatialConfig(mSchema, 100000.0, 50.0, "spatial_index");
    RewriteSpatialConfig(mSchema, 6000000.0, 50.0, "spatial_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "spatial_index:point(116.3906 39.92324)",
                             "docid=0,coordinate=116.3906 39.92324 116.3906 39.92325;"
                             "docid=1;docid=2"));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "spatial_index:point(116.3906 39.92325)",
                             "docid=0;docid=3"));

    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "spatial_index:rectangle(116.00 39.00, 117.00 40.00)",
                             "docid=0,coordinate=116.3906 39.92324 116.3906 39.92325;"
                             "docid=1;docid=2;docid=3;docid=4;docid=5"));

    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "spatial_index:polygon(-45 0, 0 0, 0 -45, -45 -45,-45 0)",
                             "docid=6;docid=7;docid=8;docid=9"));

    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "spatial_index:line(116.3906 39.92324,116.3906 39.92325,-22.5 -0.1)",
                             "docid=0;docid=1;docid=2;docid=3;"
                             "docid=4;docid=5;docid=6"));

}

void SpatialIndexInteTest::TestSearchTopLevel()
{
    string docString = "cmd=add,pk=7,coordinate=113.549132 22.198749;";
    RewriteSpatialConfig(mSchema, 100000.0, 50.0, "spatial_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "spatial_index:rectangle(0.00 0.00, 114.00 24.00)",
                             "docid=0"));
}

void SpatialIndexInteTest::RewriteSpatialConfig(const IndexPartitionSchemaPtr& schema,
        double maxSearchDist, double maxDistError, const string& indexName)
{
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    SpatialIndexConfigPtr spatialIndexConfig = DYNAMIC_POINTER_CAST(
            SpatialIndexConfig, indexSchema->GetIndexConfig(indexName));
    spatialIndexConfig->SetMaxSearchDist(maxSearchDist);
    spatialIndexConfig->SetMaxDistError(maxDistError);
}

void SpatialIndexInteTest::TestFindPointNear180DateLine()
{
    string docString = "cmd=add,pk=0,coordinate=178 39;"
                       "cmd=add,pk=1,coordinate=-178 39;"
                       "cmd=add,pk=2,coordinate=0 39;";
    RewriteSpatialConfig(mSchema, 100000000000.0, 200000.0, "spatial_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "spatial_index:rectangle(-170 30, 180 40)",
                             "docid=0;docid=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "spatial_index:rectangle(-170 30, -180 40)",
                             "docid=0;docid=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "spatial_index:rectangle(170 30, -170 40)",
                             "docid=0;docid=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "",
                             "spatial_index:rectangle(-170 30, 170 40)",
                             "docid=2"));
}

void SpatialIndexInteTest::TestLocationOnCellRightBoundary()
{
    string docString = "cmd=add,pk=0,coordinate=90 30;"
                       "cmd=add,pk=1,coordinate=45 30;";
    RewriteSpatialConfig(mSchema, 100000000000.0, 200000.0, "spatial_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "spatial_index:rectangle(45 0, 90 40)",
                             "docid=0;docid=1;"));
}

void SpatialIndexInteTest::TestFindPointsInCircle()
{
    string docString = "cmd=add,pk=0,coordinate=90 30;"
                       "cmd=add,pk=1,coordinate=45 30;";
    RewriteSpatialConfig(mSchema, 100000000000.0, 200000.0, "spatial_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "spatial_index:circle(45 29, 200000)",
                             "docid=1;"));
}

void SpatialIndexInteTest::TestFindNearPole()
{
    string docString = "cmd=add,pk=0,coordinate=180 90;"
                       "cmd=add,pk=1,coordinate=-180 90;";
    RewriteSpatialConfig(mSchema, 100000000000.0, 1, "spatial_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:point(180 90)",
                             "docid=0;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:point(-180 90)",
                             "docid=1;"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:circle(179 90,12)",
                             "docid=0;docid=1;"));
}

void SpatialIndexInteTest::TestSeekAndFilterIterator()
{
    double pLon = 1.1;
    double pLat = 0.3;
    
    uint32_t count = 0;
    stringstream ss;
    for (double lon = -0.5; lon < 0.5; lon += 0.01)
    {
        for (double lat = -0.5; lat < 0.5; lat += 0.01)
        {
            ss << "cmd=add,pk=" << count++ 
               << ",coordinate=" << pLon + lon << " " << pLat + lat << ";";
        }
    }

    string docString = ss.str();
    RewriteSpatialConfig(mSchema, 10000000.0, 100000.0, "spatial_index");
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "spatial_index:point(0.6 -0.2)", "docid=0"));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "spatial_index:point(0.600001 -0.2)", ""));

    partition::OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    IndexReaderPtr indexReader = onlinePart->GetReader()->GetIndexReader("spatial_index");
    ASSERT_TRUE(indexReader);

    Term term("circle(1.1 0.3, 22134)", "spatial_index");
    Pool pool;
    PostingIterator* iter = indexReader->Lookup(term, 1000, pt_default, &pool);
    SeekAndFilterIterator* sfIter = dynamic_cast<SeekAndFilterIterator*>(iter);
    ASSERT_TRUE(sfIter);
    PostingIterator* cloneIter = iter->Clone();
    ASSERT_TRUE(dynamic_cast<SeekAndFilterIterator*>(cloneIter));
    ASSERT_TRUE(dynamic_cast<SeekAndFilterIterator*>(cloneIter)->GetDocValueFilter());
    
    uint32_t indexIterCount = GetMatchDocCount(sfIter->GetIndexIterator());
    uint32_t preciseCount = GetMatchDocCount(cloneIter);

    ASSERT_TRUE(indexIterCount > preciseCount) << indexIterCount << "," << preciseCount;

    POOL_COMPATIBLE_DELETE_CLASS(&pool, iter);
    POOL_COMPATIBLE_DELETE_CLASS(&pool, cloneIter);
}

uint32_t SpatialIndexInteTest::GetMatchDocCount(PostingIterator* iter)
{
    int64_t begin = TimeUtility::currentTime();
    uint32_t count = 0;
    docid_t curDocId = 0;
    while (true)
    {
        curDocId = iter->SeekDoc(curDocId);
        if (curDocId == INVALID_DOCID)
        {
            break;
        }
        ++curDocId;
        ++count;
    }

    int64_t end = TimeUtility::currentTime();
    cout << "time use :" << (end - begin)/1000 << "ms" << endl;
    return count;
}


void SpatialIndexInteTest::TestMergeMulitSegment()
{
    {
        string docString = "cmd=add,pk=1,coordinate=116.3906 39.92324116.3906 39.92325;"
                           "cmd=add,pk=2,coordinate=116.3906 39.92324;"
                           "cmd=add,pk=3,coordinate=116.3906 39.92324;"
                           "cmd=add,pk=4,coordinate=116.3906 39.92325;"
                           "cmd=add,pk=5,coordinate=116.3906 39.92335;"
                           "cmd=add,pk=6,coordinate=116.3906 39.92435;"
                           "cmd=add,pk=7,coordinate=-22.5 -0.1;"
                           "cmd=add,pk=8,coordinate=-44.9 -22.5;"
                           "cmd=add,pk=9,coordinate=-22.5 -44.9;"
                           "cmd=add,pk=10,coordinate=-0.1 -22.5;";
        // RewriteSpatialConfig(mSchema, 100000.0, 50.0, "spatial_index");
        RewriteSpatialConfig(mSchema, 6000000.0, 50.0, "spatial_index");
        PartitionStateMachine psm;
        std::string mergeConfigStr
            = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"7\"}}";
        autil::legacy::FromJsonString(
            mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "spatial_index:point(116.3906 39.92324)",
            "docid=0,coordinate=116.3906 39.92324 116.3906 39.92325;"
            "docid=2;docid=4"));

        ASSERT_TRUE(psm.Transfer(
            BUILD_FULL, docString, "spatial_index:point(116.3906 39.92325)", "docid=0;docid=6"));

        ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:rectangle(116.00 39.00, 117.00 40.00)",
            "docid=0,coordinate=116.3906 39.92324 116.3906 39.92325;"
            "docid=2;docid=4;docid=6;docid=7;docid=8"));

        ASSERT_TRUE(
            psm.Transfer(QUERY, "", "spatial_index:polygon(-45 0, 0 0, 0 -45, -45 -45,-45 0)",
                "docid=1;docid=3;docid=5;docid=9"));

        ASSERT_TRUE(psm.Transfer(QUERY, "",
            "spatial_index:line(116.3906 39.92324,116.3906 39.92325,-22.5 -0.1)",
            "docid=0;docid=2;docid=4;docid=6;"
            "docid=7;docid=8;docid=9"));
    }
    FileSystemWrapper::DeleteDir(GET_TEST_DATA_PATH());
    FileSystemWrapper::MkDir(GET_TEST_DATA_PATH());
    //empty seg
    {
        string docString = "cmd=add,pk=0,coordinate=90 30;"
                           "cmd=add,pk=1,coordinate=45 30;";
        RewriteSpatialConfig(mSchema, 100000000000.0, 200000.0, "spatial_index");
        PartitionStateMachine psm;
        std::string mergeConfigStr
            = "{\"class_name\":\"test\",\"parameters\":{\"segment_count\":\"3\"}}";
        autil::legacy::FromJsonString(
                mOptions.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

        ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEST_DATA_PATH()));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString,
                             "spatial_index:rectangle(45 0, 90 40)",
                             "docid=0;docid=1;"));
    }
}


IE_NAMESPACE_END(index);

