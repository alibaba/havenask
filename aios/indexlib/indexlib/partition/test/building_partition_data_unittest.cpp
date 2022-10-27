#include "indexlib/partition/test/building_partition_data_unittest.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/in_memory_segment_creator.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/partition/in_memory_partition_data.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, BuildingPartitionDataTest);

BuildingPartitionDataTest::BuildingPartitionDataTest()
{
}

BuildingPartitionDataTest::~BuildingPartitionDataTest()
{
}

void BuildingPartitionDataTest::CaseSetUp()
{
    string field = "string1:string;";
    string index = "pk:primarykey64:string1";    
    mSchema = SchemaMaker::MakeSchema(field, index, "", "");
    mIsOnline = true;
    mMemController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
}

void BuildingPartitionDataTest::CaseTearDown()
{
}

void BuildingPartitionDataTest::TestNormalOpen()
{
    // offline, no sub
    InnerTestNormalOpen(false, false);

    // online, no sub
    InnerTestNormalOpen(false, true);

    // offline, has sub
    InnerTestNormalOpen(true, false);

    // online, has sub
    InnerTestNormalOpen(true, true);
}

void BuildingPartitionDataTest::InnerTestNormalOpen(bool hasSub, bool isOnline)
{
    IndexPartitionOptions options;
    options.SetIsOnline(isOnline);

    IndexPartitionSchemaPtr schema = CreateSchema(hasSub);
    SegmentDirectoryPtr segDir = CreateSegmentDirectory(hasSub, isOnline);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    BuildingPartitionData partData(
        options, schema, mMemController, dumpContainer);
    ASSERT_NO_THROW(partData.Open(segDir));

    InMemorySegmentPtr inMemSegment = partData.CreateNewSegment();
    InMemorySegmentPtr joinSegment = partData.CreateJoinSegment();
    
    ASSERT_TRUE(inMemSegment);

    if (isOnline)
    {
        ASSERT_TRUE(joinSegment);
        ASSERT_EQ(RealtimeSegmentDirectory::ConvertToRtSegmentId(0), 
                  inMemSegment->GetSegmentId());
        ASSERT_EQ(JoinSegmentDirectory::ConvertToJoinSegmentId(0), 
                  joinSegment->GetSegmentId());
    }
    else
    {
        ASSERT_FALSE(joinSegment);
        ASSERT_EQ((segmentid_t)0, inMemSegment->GetSegmentId());
    }

    if (hasSub)
    {
        ASSERT_TRUE(inMemSegment->GetSubInMemorySegment());
        if (joinSegment)
        {
            ASSERT_TRUE(joinSegment->GetSubInMemorySegment());
        }
    }
    else
    {
        ASSERT_FALSE(inMemSegment->GetSubInMemorySegment());
        if (joinSegment)
        {
            ASSERT_FALSE(joinSegment->GetSubInMemorySegment());
        }
    }
}

void BuildingPartitionDataTest::TestInvalidOpen()
{
    {
        // invalid open, segDir hasSub, schema not hasSub
        IndexPartitionOptions options;
        options.SetIsOnline(true);

        IndexPartitionSchemaPtr schema = CreateSchema(false);
        SegmentDirectoryPtr segDir = CreateSegmentDirectory(true, true);
        DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
        BuildingPartitionData partData(options, schema, mMemController, dumpContainer);
        ASSERT_THROW(partData.Open(segDir), misc::BadParameterException);
    }

    {
        // invalid open, segDir not hasSub, schema hasSub
        IndexPartitionOptions options;
        options.SetIsOnline(true);

        IndexPartitionSchemaPtr schema = CreateSchema(true);
        SegmentDirectoryPtr segDir = CreateSegmentDirectory(false, true);
        DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
        BuildingPartitionData partData(options, schema, mMemController, dumpContainer);
        ASSERT_THROW(partData.Open(segDir), misc::BadParameterException);
    }

    {
        // invalid open, segDir is online, options is offline
        IndexPartitionOptions options;
        options.SetIsOnline(false);

        IndexPartitionSchemaPtr schema = CreateSchema(false);
        SegmentDirectoryPtr segDir = CreateSegmentDirectory(false, true);
        DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
        BuildingPartitionData partData(options, schema, mMemController, dumpContainer);
        ASSERT_THROW(partData.Open(segDir), misc::BadParameterException);
    }

    {
        // invalid open, segDir is offline, options is online
        IndexPartitionOptions options;
        options.SetIsOnline(true);

        IndexPartitionSchemaPtr schema = CreateSchema(false);
        SegmentDirectoryPtr segDir = CreateSegmentDirectory(false, false);
        DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
        BuildingPartitionData partData(options, schema, mMemController, dumpContainer);
        ASSERT_THROW(partData.Open(segDir), misc::BadParameterException);
    }


}

void BuildingPartitionDataTest::TestDumpAndCommitVersionSuccess()
{
    mIsOnline = GET_CASE_PARAM();
    BuildingPartitionDataPtr partitionData = CreatePartitionData();
    Dump(partitionData, 100);
    partitionData->CommitVersion();
    //dump, commit

    Version version = GetNewVersion();
    Version expectVersion = MakeVersion(0, "0", 100);
    INDEXLIB_TEST_EQUAL(expectVersion, version);

    Dump(partitionData, 200);
    Dump(partitionData, 300);
    partitionData->CommitVersion();

    versionid_t versionid = 1;
    if (mIsOnline)
    {
        versionid = 2;
    }
    version = GetNewVersion();
    expectVersion = MakeVersion(versionid, "0,1,2", 300);
    INDEXLIB_TEST_EQUAL(expectVersion, version);
}

void BuildingPartitionDataTest::TestRemoveSegmentAndCommitVersionSuccess()
{
    mIsOnline = GET_CASE_PARAM();
    BuildingPartitionDataPtr partitionData = CreatePartitionData();

    Dump(partitionData, 100);
    Dump(partitionData, 200);
    partitionData->CommitVersion();

    versionid_t versionid = 0;
    if (mIsOnline)
    {
        versionid = 1;
    }
    Version version = GetNewVersion();
    Version expectVersion = MakeVersion(versionid, "0,1", 200);
    INDEXLIB_TEST_EQUAL(expectVersion, version);

    segmentid_t segId = 1;
    if (mIsOnline)
    {
        segId = RealtimeSegmentDirectory::ConvertToRtSegmentId(segId);
    }

    vector<segmentid_t> segIds;
    segIds.push_back(segId);
    partitionData->RemoveSegments(segIds);
    partitionData->CommitVersion();

    version = GetNewVersion();
    versionid++;
    expectVersion = MakeVersion(versionid, "0,1", 100);
    expectVersion.RemoveSegment(expectVersion.GetLastSegment());
    INDEXLIB_TEST_EQUAL(expectVersion, version);
}

void BuildingPartitionDataTest::TestNoCommitVersion()
{
    mIsOnline = GET_CASE_PARAM();
    BuildingPartitionDataPtr partitionData = CreatePartitionData();
    partitionData->CommitVersion();

    //not create new segment, no commit
    INDEXLIB_TEST_EQUAL(INVALID_VERSION, GetNewVersion().GetVersionId());

    InMemorySegmentPtr inMemorySegment = partitionData->CreateNewSegment();
    partitionData->CommitVersion();
    //no dump, no commit
    INDEXLIB_TEST_EQUAL(INVALID_VERSION, GetNewVersion().GetVersionId());

    Dump(partitionData);
    partitionData->CommitVersion();
    //dump, commit
    INDEXLIB_TEST_EQUAL((versionid_t)0, GetNewVersion().GetVersionId());

    partitionData->CommitVersion();
    //version no changed, no commit
    INDEXLIB_TEST_EQUAL((versionid_t)0, GetNewVersion().GetVersionId());
}

void BuildingPartitionDataTest::TestCreateJoinSegment()
{
    mIsOnline = true;
    BuildingPartitionDataPtr partitionData = CreatePartitionData();    
    InMemorySegmentPtr inMemSegment = partitionData->CreateJoinSegment();
    INDEXLIB_TEST_TRUE(inMemSegment);

    inMemSegment->EndDump();
    DirectoryPtr directory = inMemSegment->GetDirectory();
    INDEXLIB_TEST_TRUE(directory);
    INDEXLIB_TEST_TRUE(directory->GetPath().find(JOIN_INDEX_PARTITION_DIR_NAME) 
                       != string::npos);
    INDEXLIB_TEST_TRUE(inMemSegment->IsDirectoryDumped());
    INDEXLIB_TEST_EQUAL(JoinSegmentDirectory::ConvertToJoinSegmentId(0), 
                        inMemSegment->GetSegmentId());

    inMemSegment = partitionData->CreateJoinSegment();
    inMemSegment->EndDump();    

    INDEXLIB_TEST_EQUAL(JoinSegmentDirectory::ConvertToJoinSegmentId(1), 
                        inMemSegment->GetSegmentId());
}

void BuildingPartitionDataTest::TestCommitJoinVersion()
{
    mIsOnline = true;
    BuildingPartitionDataPtr partitionData = CreatePartitionData();
    //no create in mem seg, no version
    partitionData->CommitVersion();
    INDEXLIB_TEST_EQUAL(INVALID_VERSION, GetNewVersion(
                    JOIN_INDEX_PARTITION_DIR_NAME).GetVersionId());

    InMemorySegmentPtr inMemSegment = partitionData->CreateJoinSegment();
    INDEXLIB_TEST_TRUE(inMemSegment);

    //no dump, no version
    partitionData->CommitVersion();
    INDEXLIB_TEST_EQUAL(INVALID_VERSION, GetNewVersion(
                    JOIN_INDEX_PARTITION_DIR_NAME).GetVersionId());

    inMemSegment->EndDump();
    DirectoryPtr dir1 = inMemSegment->GetDirectory();
    dir1->MakeDirectory(DELETION_MAP_TASK_NAME);
    partitionData->CommitVersion();
    Version version = GetNewVersion(JOIN_INDEX_PARTITION_DIR_NAME);
    INDEXLIB_TEST_EQUAL(VersionMaker::Make(0, "", "", "0"), version);

    //no new dump, no version
    partitionData->CommitVersion();
    INDEXLIB_TEST_EQUAL(0, GetNewVersion(
                    JOIN_INDEX_PARTITION_DIR_NAME).GetVersionId());

    InMemorySegmentPtr inMemSegment2 = partitionData->CreateJoinSegment();
    INDEXLIB_TEST_TRUE(inMemSegment2);
    inMemSegment2->EndDump();
    DirectoryPtr dir2 = inMemSegment2->GetDirectory();
    dir2->MakeDirectory(DELETION_MAP_TASK_NAME);
    partitionData->CommitVersion();
    Version version2 = GetNewVersion(JOIN_INDEX_PARTITION_DIR_NAME);
    INDEXLIB_TEST_EQUAL(VersionMaker::Make(1, "", "", "1"), version2);
    
    vector<segmentid_t> segIds;
    segIds.push_back(inMemSegment2->GetSegmentId());
    partitionData->RemoveSegments(segIds);
    partitionData->CommitVersion();    

    Version version3 = GetNewVersion(JOIN_INDEX_PARTITION_DIR_NAME);
    Version expecteVersion = version2;
    expecteVersion.RemoveSegment(expecteVersion.GetLastSegment());
    expecteVersion.IncVersionId();
    INDEXLIB_TEST_EQUAL(expecteVersion, version3); 

}

void BuildingPartitionDataTest::TestCreateNewSegment()
{
    mIsOnline = GET_CASE_PARAM();
    PartitionDataMaker::MakeSegmentData(GET_PARTITION_DIRECTORY(), "0,1,5");

    BuildingPartitionDataPtr partitionData = CreatePartitionData();
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    ASSERT_TRUE(inMemSegment);

    segmentid_t expectNewSegmentId = INVALID_SEGMENTID;
    document::Locator expectLocator("");
    if (mIsOnline)
    {
        expectNewSegmentId = RealtimeSegmentDirectory::ConvertToRtSegmentId(0);
    }
    else
    {
        expectNewSegmentId = 1;
        expectLocator.SetLocator("5");
    }

    ASSERT_EQ(expectNewSegmentId, inMemSegment->GetSegmentId());
    ASSERT_EQ(expectLocator, inMemSegment->GetSegmentInfo()->locator);

    InMemorySegmentPtr inMemSegment2 = partitionData->CreateNewSegment();
    ASSERT_EQ(inMemSegment2.get(), inMemSegment.get());

    SegmentInfoPtr segInfo = inMemSegment->GetSegmentInfo();
    document::Locator newLocator("10");
    segInfo->locator = newLocator;
    segInfo->timestamp = 100;
    inMemSegment->SetStatus(InMemorySegment::DUMPING);
    inMemSegment->BeginDump();
    inMemSegment->EndDump();
    partitionData->AddBuiltSegment(
        inMemSegment->GetSegmentId(), inMemSegment->GetSegmentInfo());
    expectNewSegmentId++;
    inMemSegment->SetStatus(InMemorySegment::DUMPED);
    InMemorySegmentPtr inMemSegment3 = partitionData->CreateNewSegment();
    ASSERT_EQ(expectNewSegmentId, inMemSegment3->GetSegmentId());
    ASSERT_EQ((int64_t)100, inMemSegment3->GetSegmentInfo()->timestamp);
    ASSERT_EQ(newLocator, inMemSegment3->GetSegmentInfo()->locator);
}

BuildingPartitionDataPtr BuildingPartitionDataTest::CreatePartitionData()
{
    IndexPartitionOptions options;
    if (!mIsOnline)
    {
        options.SetIsOnline(false);
    }
    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
            GET_FILE_SYSTEM(), options, mSchema);
    return DYNAMIC_POINTER_CAST(BuildingPartitionData, partitionData);
}

void BuildingPartitionDataTest::Dump(BuildingPartitionDataPtr partitionData,
                                     timestamp_t ts)
{
    IndexPartitionOptions options;
    if (!mIsOnline)
    {
        options.SetIsOnline(false);
    }
    OfflinePartitionWriter writer(options, partitionData->mDumpSegmentContainer);
    PartitionModifierPtr modifier = 
        PartitionModifierCreator::CreatePatchModifier(
                mSchema, partitionData, true, false);
    writer.Open(mSchema, partitionData, modifier);
    modifier.reset();

    NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, 
            "cmd=add,string1=pk1");
    doc->SetTimestamp(ts);
    writer.AddDocument(doc);
    writer.DumpSegment();
    partitionData->CreateNewSegment();
    partitionData->mDumpSegmentContainer->GetInMemSegmentContainer()->EvictOldestInMemSegment();
}

Version BuildingPartitionDataTest::GetNewVersion(string dirName)
{
    //TODO: version support directory
    Version version;
    DirectoryPtr dir = GET_PARTITION_DIRECTORY();
    if (mIsOnline)
    {
        dir = dir->GetDirectory(dirName, true);
        assert(dir);
    }
    VersionLoader::GetVersion(dir, version, INVALID_VERSION);
    return version;
}

index_base::Version BuildingPartitionDataTest::MakeVersion(versionid_t vid, 
        string segmentIds, timestamp_t ts)
{
    Version expectVersion;
    if (mIsOnline)
    {
        expectVersion = VersionMaker::Make(vid, "", segmentIds, "", ts);
    }
    else
    {
        expectVersion = VersionMaker::Make(vid, segmentIds, "", "", ts);
    }
    return expectVersion;
}

IndexPartitionSchemaPtr BuildingPartitionDataTest::CreateSchema(bool hasSub)
{
    string field = "pkstr:string;text1:text;long1:uint32;multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pk:primarykey64:pkstr;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "pkstr;";

    IndexPartitionSchemaPtr schema = 
        SchemaMaker::MakeSchema(field, index, attr, summary);
    if (hasSub)
    {
        IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
                "substr1:string;subpkstr:string;sub_long:uint32;",
                "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
                "substr1;subpkstr;sub_long;",
                "");
        schema->SetSubIndexPartitionSchema(subSchema);
        SchemaRewriter::RewriteForSubTable(schema);
    }
    return schema;
}

SegmentDirectoryPtr BuildingPartitionDataTest::CreateSegmentDirectory(
        bool hasSub, bool isOnline)
{
    IndexPartitionOptions options;
    options.SetIsOnline(isOnline);

    return SegmentDirectoryCreator::Create(options, GET_PARTITION_DIRECTORY(), 
            INVALID_VERSION, hasSub);
}

void BuildingPartitionDataTest::TestUpdatePartitionInfoWithSub()
{
    IndexPartitionOptions options;
    options.SetIsOnline(true);

    IndexPartitionSchemaPtr schema = CreateSchema(true);
    SegmentDirectoryPtr segDir = CreateSegmentDirectory(true, true);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    BuildingPartitionDataPtr partData(new BuildingPartitionData(options, schema, mMemController, dumpContainer));
    partData->Open(segDir);
    partData->CreateNewSegment();

    CheckPartitionInfo(partData, 0, 0);
    PartitionInfoPtr lastPartInfo = partData->GetPartitionInfo();
    PartitionInfoPtr lastSubPartInfo = lastPartInfo->GetSubPartitionInfo();


    OnlinePartitionWriter writer(options, dumpContainer);
    OpenWriter(writer, schema, options, partData);

    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1^sub2,substr1=s1^s2;"
                       "cmd=update_field,pkstr=hello,long1=10,ts=1;"
                       "cmd=delete,pkstr=hello,ts=2;";
    vector<NormalDocumentPtr> docs = 
        DocumentCreator::CreateDocuments(schema, docString);

    // check updatePartitionInfo after add
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    CheckPartitionInfo(partData, 1, 2);

    ASSERT_TRUE(partData->GetPartitionInfo().get() != lastPartInfo.get());
    ASSERT_TRUE(partData->GetPartitionInfo()->GetSubPartitionInfo().get() != lastSubPartInfo.get());
    lastPartInfo = partData->GetPartitionInfo();
    lastSubPartInfo = lastPartInfo->GetSubPartitionInfo();

    // check updatePartitionInfo after update, not update partinfo
    ASSERT_TRUE(writer.BuildDocument(docs[1]));
    CheckPartitionInfo(partData, 1, 2);

    ASSERT_TRUE(partData->GetPartitionInfo().get() == lastPartInfo.get());
    ASSERT_TRUE(partData->GetPartitionInfo()->GetSubPartitionInfo().get() == lastSubPartInfo.get());
    lastPartInfo = partData->GetPartitionInfo();
    lastSubPartInfo = lastPartInfo->GetSubPartitionInfo();

    // check updatePartitionInfo after delete, not update partinfo
    ASSERT_TRUE(writer.BuildDocument(docs[2]));
    CheckPartitionInfo(partData, 1, 2);

    ASSERT_TRUE(partData->GetPartitionInfo().get() == lastPartInfo.get());
    ASSERT_TRUE(partData->GetPartitionInfo()->GetSubPartitionInfo().get() == lastSubPartInfo.get());
}

void BuildingPartitionDataTest::TestGetDeletionMapReaderWithSub()
{
    IndexPartitionOptions options;
    options.SetIsOnline(true);

    IndexPartitionSchemaPtr schema = CreateSchema(true);
    SegmentDirectoryPtr segDir = CreateSegmentDirectory(true, true);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    BuildingPartitionDataPtr partData(new BuildingPartitionData(options, schema, mMemController, dumpContainer));
    partData->Open(segDir);
    partData->CreateNewSegment();


    OnlinePartitionWriter writer(options, dumpContainer);
    OpenWriter(writer, schema, options, partData);

    string docString = "cmd=add,pkstr=hello,ts=0,subpkstr=sub1^sub2,substr1=s1^s2;"
                       "cmd=delete,pkstr=hello,ts=2;";
    vector<NormalDocumentPtr> docs = 
        DocumentCreator::CreateDocuments(schema, docString);

    DeletionMapReaderPtr deletionMapReader = partData->GetDeletionMapReader();
    DeletionMapReaderPtr subDeletionMapReader = 
        partData->GetSubPartitionData()->GetDeletionMapReader();

    CheckDeletionMapReader(deletionMapReader, "0", true);
    CheckDeletionMapReader(subDeletionMapReader, "0", true);

    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    CheckDeletionMapReader(deletionMapReader, "0", false);
    CheckDeletionMapReader(deletionMapReader, "1", true);
    CheckDeletionMapReader(subDeletionMapReader, "0,1", false);
    CheckDeletionMapReader(subDeletionMapReader, "2", true);

    // repeated add
    ASSERT_TRUE(writer.BuildDocument(docs[0]));
    CheckDeletionMapReader(deletionMapReader, "0,2", true);
    CheckDeletionMapReader(deletionMapReader, "1", false);
    CheckDeletionMapReader(subDeletionMapReader, "0,1,4", true);
    CheckDeletionMapReader(subDeletionMapReader, "2,3", false);

    // delete
    ASSERT_TRUE(writer.BuildDocument(docs[1]));
    CheckDeletionMapReader(deletionMapReader, "0,1,2", true);
    CheckDeletionMapReader(subDeletionMapReader, "0,1,2,3,4", true);
}

void BuildingPartitionDataTest::TestClone()
{
    IndexPartitionSchemaPtr schema = CreateSchema(true);
    IndexPartitionOptions options;
    options.SetIsOnline(true);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    BuildingPartitionDataPtr buildingPartData(new BuildingPartitionData(
                    options, schema, mMemController, dumpContainer));
    SegmentDirectoryPtr segDir = CreateSegmentDirectory(true, true);
    buildingPartData->Open(segDir);
    BuildingPartitionDataPtr cloneData = BuildingPartitionDataPtr(buildingPartData->Clone());
    ASSERT_TRUE(buildingPartData->mInMemPartitionData != cloneData->mInMemPartitionData);
    ASSERT_TRUE(buildingPartData->mDumpSegmentContainer != cloneData->mDumpSegmentContainer);
    ASSERT_EQ(cloneData->mInMemPartitionData->mDumpSegmentContainer,
              cloneData->mDumpSegmentContainer);
    InMemoryPartitionData* subData = 
        cloneData->mInMemPartitionData->GetSubInMemoryPartitionData();
    ASSERT_EQ(subData->mDumpSegmentContainer,
              cloneData->mDumpSegmentContainer);
}


void BuildingPartitionDataTest::TestCreateSegmentIterator()
{
    
    IndexPartitionSchemaPtr schema = CreateSchema(true);
    IndexPartitionOptions options;
    options.SetIsOnline(true);
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    Version onDiskVersion = VersionMaker::Make(
            GET_PARTITION_DIRECTORY(), 1, "", "0,2,4", "", 0, true);

    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    PartitionDataPtr buildingPartData(
            new BuildingPartitionData(options, schema, mMemController, dumpContainer));

    SegmentDirectoryPtr segDir(new OnlineSegmentDirectory(
                    options.GetOnlineConfig().enableRecoverIndex));
    segDir->Init(GET_PARTITION_DIRECTORY(), INVALID_VERSION, true);
    
    BuildingPartitionDataPtr buildData = DYNAMIC_POINTER_CAST(BuildingPartitionData, buildingPartData);
    buildData->Open(segDir);
    
    InMemorySegmentPtr inMemorySegment = buildingPartData->CreateNewSegment();
    NormalSegmentDumpItemPtr item(new NormalSegmentDumpItem(options, schema, ""));
    item->Init(NULL, buildingPartData, PartitionModifierPtr(), FlushedLocatorContainerPtr(), false);
    dumpContainer->PushDumpItem(item);
    
    vector<InMemorySegmentPtr> buildingSegments;
    dumpContainer->GetDumpingSegments(buildingSegments);
    auto segment = buildingPartData->CreateNewSegment();
    buildingSegments.push_back(segment);
    
    auto ite = buildingPartData->CreateSegmentIterator();
    ASSERT_TRUE(ite);
    ASSERT_EQ(ite->mBuildingSegments, buildingSegments);
    ASSERT_EQ(ite->mNextBuildingSegmentId, segment->GetSegmentId() + 1);
    ASSERT_EQ(ite->mBuildingSegmentId, segment->GetSegmentId());

    // test sub in_memory_data create iterator
    vector<InMemorySegmentPtr> subSegments;
    dumpContainer->GetSubDumpingSegments(subSegments);
    subSegments.push_back(segment->GetSubInMemorySegment());
    auto subite = buildingPartData->GetSubPartitionData()->CreateSegmentIterator();
    ASSERT_TRUE(subite);
    ASSERT_EQ(subite->mBuildingSegments, subSegments);
    ASSERT_EQ(subite->mNextBuildingSegmentId, segment->GetSubInMemorySegment()->GetSegmentId() + 1);
    ASSERT_EQ(subite->mBuildingSegmentId, segment->GetSubInMemorySegment()->GetSegmentId());

}


void BuildingPartitionDataTest::TestInitInMemorySegmentCreator()
{
    InnerTestInitInMemorySegmentCreator(false, true, true);
    InnerTestInitInMemorySegmentCreator(false, false, false);
    InnerTestInitInMemorySegmentCreator(true, true, true);
    InnerTestInitInMemorySegmentCreator(true, false, false);
}

void BuildingPartitionDataTest::InnerTestInitInMemorySegmentCreator(
        bool hasSub, bool isOnline, bool expectUseRtOption)
{
    IndexPartitionSchemaPtr schema = CreateSchema(hasSub);

    IndexPartitionOptions options;
    options.SetIsOnline(isOnline);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    BuildingPartitionData buildingPartData(options, schema, mMemController, dumpContainer);
    SegmentDirectoryPtr segDir = CreateSegmentDirectory(hasSub, isOnline);
    buildingPartData.Open(segDir);
    ASSERT_EQ(expectUseRtOption, 
              buildingPartData.mInMemorySegmentCreator->mOptions.IsOnline());
}

void BuildingPartitionDataTest::CheckPartitionInfo(
        const PartitionDataPtr& partData,
        docid_t docCount, docid_t subDocCount)
{
    PartitionInfoPtr partitionInfo = partData->GetPartitionInfo();
    PartitionInfoPtr subPartitionInfo = partitionInfo->GetSubPartitionInfo();

    ASSERT_TRUE(partitionInfo);
    ASSERT_EQ(docCount, partitionInfo->GetTotalDocCount());    

    ASSERT_TRUE(subPartitionInfo);
    ASSERT_EQ(subDocCount, subPartitionInfo->GetTotalDocCount());    

    PartitionInfoPtr partInfoInSubPartData = 
        partData->GetSubPartitionData()->GetPartitionInfo();
    ASSERT_EQ(subPartitionInfo.get(), partInfoInSubPartData.get());
    ASSERT_FALSE(partInfoInSubPartData->GetSubPartitionInfo());
}

void BuildingPartitionDataTest::CheckDeletionMapReader(
        const DeletionMapReaderPtr& deletionMapReader,
        const string& docIdStrs, bool isDeleted)
{
    ASSERT_TRUE(deletionMapReader);
    vector<docid_t> docIds;
    StringUtil::fromString(docIdStrs, docIds, ",");
    
    for (size_t i = 0; i < docIds.size(); i++)
    {
        if (isDeleted)
        {
            ASSERT_TRUE(deletionMapReader->IsDeleted(docIds[i]));
        }
        else
        {
            ASSERT_FALSE(deletionMapReader->IsDeleted(docIds[i]));
        }
    }
}

void BuildingPartitionDataTest::OpenWriter(
        OnlinePartitionWriter& writer, 
        const IndexPartitionSchemaPtr& schema, 
        const IndexPartitionOptions& options,
        const PartitionDataPtr& partitionData)
{
    IndexPartitionReaderPtr reader(new OnlinePartitionReader(
                    options, schema, util::SearchCachePartitionWrapperPtr()));
    reader->Open(partitionData);
    PartitionModifierPtr modifier = 
        PartitionModifierCreator::CreateInplaceModifier(schema, reader);
    writer.Open(schema, partitionData, modifier);
}


IE_NAMESPACE_END(partition);

