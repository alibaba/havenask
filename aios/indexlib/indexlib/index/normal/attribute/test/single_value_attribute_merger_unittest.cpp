#include <autil/StringTokenizer.h>
#include "indexlib/misc/exception.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/file_system/directory_creator.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);

class FakeSegmentDirectory : public merger::SegmentDirectory
{
public:
    FakeSegmentDirectory(const std::string root, const Version& version)
        : merger::SegmentDirectory(root, version)
    {
        Version::Iterator verIt = version.CreateIterator();
        while (verIt.HasNext())
        {
            mSegmentIds.insert(verIt.Next());
        }
    }

public:
    void ListAttrPatch(
            const std::string& attrName, segmentid_t segId, 
            std::vector<std::pair<std::string, segmentid_t> >& patchs) const override
    {
        if (mSegmentIds.find(segId) == mSegmentIds.end())
        {
            INDEXLIB_THROW(misc::RuntimeException, "list patches fail!");
        }
        merger::SegmentDirectory::ListAttrPatch(attrName, segId, patchs);
    }

public:
    std::set<segmentid_t> mSegmentIds;
};

class FakeUInt32AttributeSegmentReader : public AttributeSegmentReader
{
public:
    FakeUInt32AttributeSegmentReader(const vector<uint32_t>& data)
        : mData(data)
    {}

    virtual bool IsInMemory() const { return true; }
    virtual uint32_t GetDataLength(docid_t docId) const { return sizeof(uint32_t); }
    virtual uint64_t GetOffset(docid_t docId) const { return sizeof(uint32_t) * docId; }
    virtual bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) 
    {
        assert(docId < (docid_t)mData.size());
        uint32_t& value = *(uint32_t*)buf;
        value = mData[docId];
        return true;
    }
    
private:
    vector<uint32_t> mData;
};

class FakeInMemorySegmentReaderForSVAM : public InMemorySegmentReader
{
public:
    FakeInMemorySegmentReaderForSVAM(const vector<uint32_t>& attrSegData)
        : mAttributeSegmentReader(
                new FakeUInt32AttributeSegmentReader(attrSegData))
    {   
    }

    virtual AttributeSegmentReaderPtr GetAttributeSegmentReader(const string& name)    
    {
        return mAttributeSegmentReader;
    }

private:
    AttributeSegmentReaderPtr mAttributeSegmentReader;
};

class SingleValueAttributeMergerTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH() + "/";
        mAttrConfig = AttributeTestUtil::CreateAttrConfig<uint32_t>();
        mAttrConfig->GetFieldConfig()->ClearCompressType();
        srand(8888);
    }

    void CaseTearDown() override
    {
    }

    AttributeConfigPtr CreateCompressAtttributeConfig()
    {
        AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<uint32_t>();
        FieldConfigPtr fieldConfig = attrConfig->GetFieldConfig();
        fieldConfig->SetCompressType("equal");
        return attrConfig;
    }

    void TestCaseForMergeWithNoDeletedDoc()
    {
        vector<uint32_t> docCounts;
        AttributeTestUtil::CreateDocCounts(1, docCounts);
        vector<set<docid_t> > delDocIdSets;
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);

        
        ReclaimMapPtr reclaimMap;
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false);
        // with multi output segment
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false, 2);

        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge(docCounts, delDocIdSets,  mAttrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false, 2);
        
        docCounts.push_back(0);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false, 2);
    }
        
    void TestCaseForMergeWithDeletedDocs()
    {
        vector<uint32_t> docCounts;
        vector<set<docid_t> > delDocIdSets;
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false, 2);

        docCounts.push_back(0);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false, 2);
    }

    void TestCaseForSortMergeWithNoDeleteDoc()
    {
        vector<set<docid_t> > delDocIdSets;
        vector<uint32_t> docCounts;
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);

        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, true);
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, true, 2);

        docCounts.push_back(0);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, false, 2);
    }

    void TestCaseForSortMergeWithDeleteDocs()
    {
        vector<set<docid_t> > delDocIdSets;
        vector<uint32_t> docCounts;
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);

        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, true);
        InnerTestMerge(docCounts, delDocIdSets, mAttrConfig, true, 2);
        
        docCounts.push_back(0);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        InnerTestMerge(docCounts, delDocIdSets,  mAttrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets,  mAttrConfig, false, 2);
    }

    void TestCaseForMergeWithNoDeletedDocAndCompressData()
    {
        AttributeConfigPtr attrConfig = CreateCompressAtttributeConfig();
        vector<uint32_t> docCounts;
        AttributeTestUtil::CreateDocCounts(1, docCounts);
        vector<set<docid_t> > delDocIdSets;
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);

        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, false, 2);

        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);

        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, false, 2);

        docCounts.push_back(0);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, false, 2);
    }

    void TestCaseForMergeWithDeletedDocsAndCompressData()
    {
        AttributeConfigPtr attrConfig = CreateCompressAtttributeConfig();
        vector<uint32_t> docCounts;
        vector<set<docid_t> > delDocIdSets;
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        
        InnerTestMerge(docCounts, delDocIdSets, attrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets, attrConfig, false, 2);

        docCounts.push_back(0);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        InnerTestMerge(docCounts, delDocIdSets, attrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets, attrConfig, false, 2);
    }

    void TestCaseForSortMergeWithNoDeleteDocAndCompressData()
    {
        AttributeConfigPtr attrConfig = CreateCompressAtttributeConfig();
        vector<set<docid_t> > delDocIdSets;
        vector<uint32_t> docCounts;
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);

        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, true);
        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, true, 2);

        docCounts.push_back(0);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, false, 2);
    }

    void TestCaseForSortMergeWithDeleteDocsAndCompressData()
    {
        AttributeConfigPtr attrConfig = CreateCompressAtttributeConfig();
        vector<set<docid_t> > delDocIdSets;
        vector<uint32_t> docCounts;
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);

        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, true);
        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, true, 2);

        docCounts.push_back(0);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, false);
        InnerTestMerge(docCounts, delDocIdSets,  attrConfig, false, 2);
    }

    void TestCaseForMergeAllWithUpdate()
    {
        string mergeString = "0,1,2";
        string patchString = "1:0;2:0,1";
        InnerTestMergeWithUpdate(3, mergeString, patchString, false);
        InnerTestMergeWithUpdate(3, mergeString, patchString, true);
    }

    void TestCaseForMergePartialWithUpdate()
    {
        // seg 1 has 0.patch, but seg 1 is not merged.
        string mergeString = "0,2";
        string patchString = "1:0;2:0";
        InnerTestMergeWithUpdate(3, mergeString, patchString, false);
        InnerTestMergeWithUpdate(3, mergeString, patchString, true);

        //total 5 segments, does not merge the last segment
        mergeString = "1,3";
        patchString = "1:0;2:0,1;3:0,1,2;4:0,1,2,3";
        InnerTestMergeWithUpdate(5, mergeString, patchString, false);
        InnerTestMergeWithUpdate(5, mergeString, patchString, true);
    }

    void TestCaseForMergePartialWithUpdateAndPatchMerge()
    {
        size_t docCount = 3;
        string mergeString = "0,2";
        string patchString = "2:1";
        InnerTestMergeWithUpdate(docCount, mergeString, patchString, false);
        InnerTestMergeWithUpdate(docCount, mergeString, patchString, true);

        docCount = 4;
        mergeString = "0,2";
        patchString = "2:1;3:1";
        InnerTestMergeWithUpdate(docCount, mergeString, patchString, false);
        InnerTestMergeWithUpdate(docCount, mergeString, patchString, true);

        docCount = 4;
        mergeString = "0,2,3";
        patchString = "1:0;2:0,1;3:1";
        InnerTestMergeWithUpdate(docCount, mergeString, patchString, false);
        InnerTestMergeWithUpdate(docCount, mergeString, patchString, true);

        docCount = 5;
        mergeString = "1,3";
        patchString = "2:0;3:2;4:3";
        InnerTestMergeWithUpdate(docCount, mergeString, patchString, false);
        InnerTestMergeWithUpdate(docCount, mergeString, patchString, true);

        docCount = 5;
        mergeString = "1,3";
        patchString = "1:0;2:0,1;3:0,1,2;4:0,1,2,3";
        InnerTestMergeWithUpdate(docCount, mergeString, patchString, false);
        InnerTestMergeWithUpdate(docCount, mergeString, patchString, true);
    }

    void TestCaseForPerformanceOfMergePartialWithUpdate()
    {

        // total has 6 segments(0, 2, 3, 5, 7, 8), only merge 3 and 5
        string mergeString = "3,5";
        string patchString = "2:0;3:0,2;5:0,2,3;7:0,2,3,5;8:0,2,3,5,7";
        set<segmentid_t> invalidSegmentIds;
        invalidSegmentIds.insert(1);
        invalidSegmentIds.insert(4);
        invalidSegmentIds.insert(6);

        size_t segCount = 9;
        vector<uint32_t> docCounts;
        AttributeTestUtil::CreateDocCounts(segCount, docCounts);

        vector<set<docid_t> > delDocIdSets;
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);

        ReclaimMapPtr reclaimMap;
        reclaimMap = CreateReclaimMapForUpdateCase(docCounts, delDocIdSets, mergeString);

        vector<vector<uint32_t> > multiSegmentData;
        CreateMultiSegmentData(docCounts, multiSegmentData);
        BuildData(multiSegmentData, mAttrConfig, invalidSegmentIds);
        CreatePatches(patchString, docCounts, multiSegmentData);
        
        SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos(
                docCounts, delDocIdSets, mergeString);

        string segmentDir, attrPath;
        PrepareMergeDestAttrPath(segCount, segmentDir, attrPath);

        Version version;
        for (uint32_t i = 0; i < segCount; i++)
        {
            if (invalidSegmentIds.find(i) != invalidSegmentIds.end())
            {
                continue;
            }
            version.AddSegment(i);
        }

        merger::SegmentDirectoryPtr segDir(new FakeSegmentDirectory(mDir, version));
        segDir->Init(false);

        SingleValueAttributeMerger<uint32_t> attrMerger(true);
        attrMerger.Init(mAttrConfig);
        attrMerger.BeginMerge(segDir);

        MergerResource resource;
        resource.targetSegmentCount = 1;
        resource.reclaimMap = reclaimMap;
        OutputSegmentMergeInfo segInfo;
        segInfo.targetSegmentIndex = 0;
        segInfo.path = attrPath;
        segInfo.directory = DirectoryCreator::Create(segInfo.path);

        try 
        {
            // should not throw exception if logic is right
            attrMerger.Merge(resource, segMergeInfos, {segInfo});
        }
        catch(const ExceptionBase& e)
        {
            INDEXLIB_TEST_TRUE(false);
        }
    }

private:
    void InnerTestMerge(const vector<uint32_t> &docCounts,
                        const vector<set<docid_t> > &delDocIdSets,
                        const AttributeConfigPtr &attrConfig, bool sortMerge,
                        uint32_t targetSegmentCount = 1)
    {

        TearDown();
        SetUp();

        vector<vector<uint32_t> > multiSegmentData;
        CreateMultiSegmentData(docCounts, multiSegmentData);
        BuildData(multiSegmentData, attrConfig);

        SegmentMergeInfos segMergeInfos =
            CreateSegmentMergeInfos(docCounts, delDocIdSets);
        ReclaimMapPtr reclaimMap;
        vector<docid_t> baseDocIds = IndexTestUtil::GetTargetBaseDocIds(
            docCounts, delDocIdSets, segMergeInfos, targetSegmentCount);
        if (sortMerge) {
          reclaimMap = IndexTestUtil::CreateSortMergingReclaimMap(
              docCounts, delDocIdSets, baseDocIds);
        } else {
          reclaimMap = IndexTestUtil::CreateReclaimMap(docCounts, delDocIdSets,
                                                       false, baseDocIds);
        }

        MergeAttribute(docCounts, delDocIdSets, reclaimMap, segMergeInfos,
                       attrConfig, sortMerge, targetSegmentCount);

        SingleValueAttributeReader<uint32_t> attrReader;
        segmentid_t mergeSegmentId = docCounts.size();
        Version version = IndexTestUtil::CreateVersion(targetSegmentCount, mergeSegmentId);

        index_base::PartitionDataPtr partitionDataPtr = 
            test::PartitionDataMaker::CreateSimplePartitionData(
                    GET_FILE_SYSTEM(), mDir, version);

        INDEXLIB_TEST_TRUE(attrReader.Open(attrConfig, partitionDataPtr));

        Check(attrReader, multiSegmentData, reclaimMap, segMergeInfos);
    }

    void InnerTestMergeWithUpdate(size_t segCount, const string& mergeString,
                                  const string& patchString, bool withDel)
    {
        vector<uint32_t> docCounts;
        AttributeTestUtil::CreateDocCounts(segCount, docCounts);

        vector<set<docid_t> > delDocIdSets;
        if (withDel)
        {
            AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        }
        else
        {
            AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        }

        DoTestMergeWithUpdate(docCounts, delDocIdSets, mergeString,
                              patchString);
        DoTestMergeWithUpdate(docCounts, delDocIdSets, mergeString,
                              patchString, 2);
    }

    void DoTestMergeWithUpdate(const vector<uint32_t>& docCounts, 
                               const vector<set<docid_t> >& delDocIdSets,
                               const string& mergeString,
                               const string& patchString, uint32_t targetSegmentCount = 1)
    {
        TearDown();
        SetUp();

        vector<vector<uint32_t> > multiSegmentData;
        CreateMultiSegmentData(docCounts, multiSegmentData);
        BuildData(multiSegmentData, mAttrConfig);
        CreatePatches(patchString, docCounts, multiSegmentData);

        SegmentMergeInfos segMergeInfos =
            CreateSegmentMergeInfos(docCounts, delDocIdSets, mergeString);
        vector<docid_t> baseDocIds = IndexTestUtil::GetTargetBaseDocIds(
            docCounts, delDocIdSets, segMergeInfos, targetSegmentCount);
        ReclaimMapPtr reclaimMap;
        reclaimMap = CreateReclaimMapForUpdateCase(docCounts, delDocIdSets, mergeString, baseDocIds);
        

        MergeAttribute(docCounts, delDocIdSets, 
                       reclaimMap, segMergeInfos, mAttrConfig, false, targetSegmentCount);

        SingleValueAttributeReader<uint32_t> attrReader;
        segmentid_t mergeSegmentId = docCounts.size();
        Version version = CreateMergedVersion(mergeSegmentId + targetSegmentCount - 1, mergeString);

        index_base::PartitionDataPtr partitionDataPtr = 
            test::PartitionDataMaker::CreateSimplePartitionData(
                    GET_FILE_SYSTEM(), mDir, version);
        INDEXLIB_TEST_TRUE(attrReader.Open(mAttrConfig, partitionDataPtr));
        PatchLoader::LoadAttributePatch(attrReader, mAttrConfig, partitionDataPtr);

        map<docid_t, docid_t> globalIdMap;
        CreateGlobalIDMap(docCounts, delDocIdSets, mergeString, globalIdMap);
        CheckForUpdate(attrReader, multiSegmentData, globalIdMap, segMergeInfos);
    }

    void MergeAttribute(const vector<uint32_t>& docCounts, 
                        const vector<set<docid_t> >& delDocIdSets,
                        const ReclaimMapPtr& reclaimMap,
                        const SegmentMergeInfos& segMergeInfos,
                        const AttributeConfigPtr& attrConfig,
                        bool sortMerge,
                        size_t targetSegmentCount = 1)
    {
        uint32_t segmentCount = docCounts.size();
        vector<string> segmentDirs(targetSegmentCount);
        vector<string> attrPaths(targetSegmentCount);

        OutputSegmentMergeInfos outputSegs(targetSegmentCount);
        for (uint32_t i = 0; i < targetSegmentCount; ++i) {
            PrepareMergeDestAttrPath(segmentCount + i, segmentDirs[i], attrPaths[i]);
            outputSegs[i].targetSegmentIndex = i;
            outputSegs[i].path = attrPaths[i];
            outputSegs[i].directory = DirectoryCreator::Create(outputSegs[i].path);
        }
        
        merger::SegmentDirectoryPtr segDir =
            IndexTestUtil::CreateSegmentDirectory(mDir, segmentCount);
        segDir->Init(false);

        SingleValueAttributeMerger<uint32_t> attrMerger(true);
        attrMerger.Init(attrConfig);
        attrMerger.BeginMerge(segDir);

        MergerResource resource;
        resource.targetSegmentCount = targetSegmentCount;
        resource.reclaimMap = reclaimMap;

        if (sortMerge) {
            attrMerger.SortByWeightMerge(resource, segMergeInfos, outputSegs);
        }
        else {
            attrMerger.Merge(resource, segMergeInfos, outputSegs);
        }
        
        GenerateNewSegmentInfo(docCounts, delDocIdSets, segMergeInfos, segmentDirs, targetSegmentCount);
    }

    void PrepareMergeDestAttrPath(segmentid_t destSegmentId, string& segmentDir,
                                  string& attrPath)
    {
        GetMergeDestAttrPath(destSegmentId, segmentDir, attrPath);
        IndexTestUtil::ResetDir(segmentDir);
        IndexTestUtil::MkDir(attrPath);
    }

    void GetMergeDestAttrPath(segmentid_t destSegmentId, string& segmentDir,
                              string& attrPath)
    {
        stringstream segmentSS;
        segmentid_t mergeSegmentId = destSegmentId;
        segmentSS << mDir << SEGMENT_FILE_NAME_PREFIX << "_" 
                  << mergeSegmentId << "_level_0/";
        segmentDir = segmentSS.str();
        attrPath = segmentDir + ATTRIBUTE_DIR_NAME + "/";
    }

    void GenerateNewSegmentInfo(const vector<uint32_t> &docCounts,
                                const vector<set<docid_t> > &delDocIdSets,
                                const SegmentMergeInfos &segMergeInfos,
                                const vector<string> &segmentDirs,
                                uint32_t targetSegmentCount = 1) {
      uint32_t total = GetTotalDocCount(docCounts, delDocIdSets, segMergeInfos);
      uint32_t splitFactor = total / targetSegmentCount;
        for (uint32_t i = 0; i < targetSegmentCount; ++i) {
            SegmentInfo segInfo;
            segInfo.docCount = splitFactor;
            if (i == targetSegmentCount - 1) {
                segInfo.docCount = total - (splitFactor*i);
            }
            segInfo.Store(FileSystemWrapper::JoinPath(segmentDirs[i], SEGMENT_INFO_FILE_NAME));
        }
    }

    void CreateMultiSegmentData(const vector<uint32_t>& docCounts, 
                                vector<vector<uint32_t> >& multiSegmentData)
    {
        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            uint32_t docCount = docCounts[i];
            vector<uint32_t> segmentData;
            CreateSegmentData(docCount, segmentData);
            multiSegmentData.push_back(segmentData);
        }
    }

    void CreateSegmentData(uint32_t docCount, vector<uint32_t>& segmentData)
    {
        for (size_t i = 0; i < docCount; ++i)
        {
            segmentData.push_back(rand() % 10);
        }
    }

    SegmentMergeInfos CreateSegmentMergeInfos(const vector<uint32_t>& docCounts,
            const vector<set<docid_t> >& delDocIdSets)
    {
        SegmentMergeInfos segMergeInfos;
        docid_t baseDocId = 0;
        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            SegmentMergeInfo mergeInfo;
            mergeInfo.segmentId = i;
            mergeInfo.segmentInfo.docCount = docCounts[i];
            mergeInfo.deletedDocCount = delDocIdSets[i].size();
            mergeInfo.baseDocId = baseDocId;
            segMergeInfos.push_back(mergeInfo);

            baseDocId += docCounts[i];
        }
        return segMergeInfos;
    }

    uint32_t GetTotalDocCount(const vector<uint32_t> &docCounts,
                              const vector<set<docid_t> > &delDocIdSets,
                              const SegmentMergeInfos &segMergeInfos) {

        vector<uint32_t> mergeDocCounts;
        vector<set<docid_t> > mergeDelDocIdSets;
        for (size_t i = 0; i < segMergeInfos.size(); ++i)
        {
            segmentid_t segId = segMergeInfos[i].segmentId;
            mergeDocCounts.push_back(docCounts[segId]);
            mergeDelDocIdSets.push_back(delDocIdSets[segId]);
        }
        uint32_t total = 0;
        for (size_t i = 0; i < mergeDocCounts.size(); ++i)
        {
            total += mergeDocCounts[i] - mergeDelDocIdSets[i].size();
        }
        return total;
    }

    void BuildData(vector<vector<uint32_t> >& multiSegmentData, 
                   const AttributeConfigPtr& attrConfig,
                   set<segmentid_t> invalidSegmentIds = set<segmentid_t>())
    {
        for (size_t i = 0; i < multiSegmentData.size(); ++i)
        {
            segmentid_t id = i;
            if (invalidSegmentIds.find(id) != invalidSegmentIds.end())
            {
                continue;
            }

            stringstream segPath;
            segPath << mDir << SEGMENT_FILE_NAME_PREFIX << "_" << id << "_level_0/";
            IndexTestUtil::ResetDir(segPath.str());
            string attrPath = segPath.str() + ATTRIBUTE_DIR_NAME + "/";
            IndexTestUtil::ResetDir(attrPath);
            
            SingleValueAttributeWriter<uint32_t> writer(attrConfig);
            AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()
                    ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
            writer.SetAttrConvertor(convertor);
            writer.Init(FSWriterParamDeciderPtr(), NULL);
            for (size_t j = 0; j < multiSegmentData[i].size(); ++j)
            {
                stringstream value;
                value << multiSegmentData[i][j];
                autil::ConstString encodeValue = convertor->Encode(autil::ConstString(value.str()), &mPool);
                writer.AddField(j, encodeValue);
            }
            
            AttributeWriterHelper::DumpWriter(writer, attrPath);
            SegmentInfo segInfo;
            segInfo.docCount = multiSegmentData[i].size();
            segInfo.Store(segPath.str() + SEGMENT_INFO_FILE_NAME);
        }
    }

    void Check(const SingleValueAttributeReader<uint32_t>& reader, 
               const vector<vector<uint32_t> >& answer,
               const ReclaimMapPtr& reclaimMap,
               const SegmentMergeInfos& segMergeInfos)
    {
        for (size_t i = 0; i < answer.size(); ++i)
        {
            const vector<uint32_t>& answerSegment = answer[i];
            for (size_t j = 0; j < answerSegment.size(); ++j)
            {
                string strValue;
                docid_t newId = reclaimMap->GetNewId(j +
                        segMergeInfos[i].baseDocId);
                if (newId == INVALID_DOCID)
                {
                    continue;
                }
                
                reader.Read(newId, strValue);
                istringstream iss(strValue);
                uint32_t value;
                iss >> value;
                INDEXLIB_TEST_EQUAL(answerSegment[j], value);
            }
        }
    }

    void CheckForUpdate(const SingleValueAttributeReader<uint32_t>& reader, 
                        const vector<vector<uint32_t> >& answer,
                        map<docid_t, docid_t>& globalIdMap,
                        const SegmentMergeInfos& segMergeInfos)
    {
        docid_t baseDocId = 0;
        for (size_t i = 0; i < answer.size(); ++i)
        {
            const vector<uint32_t>& answerSegment = answer[i];
            for (size_t j = 0; j < answerSegment.size(); ++j)
            {
                string strValue;
                docid_t newId = globalIdMap[j + baseDocId];
                if (newId == INVALID_DOCID)
                {
                    continue;
                }
                
                reader.Read(newId, strValue);
                istringstream iss(strValue);
                uint32_t value;
                iss >> value;
                INDEXLIB_TEST_EQUAL(answerSegment[j], value);
            }
            baseDocId += answerSegment.size();
        }
    }


    InMemorySegmentReaderPtr CreateInMemorySegmentReader(
            const vector<uint32_t>& attrSegData)
    {
        return InMemorySegmentReaderPtr(
                new FakeInMemorySegmentReaderForSVAM(attrSegData));
    }


    // patch string format : "1:0;2:0,1..."  
    void CreatePatches(const string& patchString, 
                       const vector<uint32_t>& docCounts,
                       vector<vector<uint32_t> >& answer)
    {
        StringTokenizer st;
        st.tokenize(patchString, ";");
        for (size_t i = 0; i < st.getNumTokens(); ++i) 
        {
            StringTokenizer st2;
            st2.tokenize(st[i], ":");

            segmentid_t storedSegId = StringUtil::fromString<segmentid_t>(st2[0]);
            string patchedSegIdList = st2[1];
            StringTokenizer st3;
            st3.tokenize(patchedSegIdList, ",");
            for (size_t j = 0; j < st3.getNumTokens(); ++j)
            {
                segmentid_t patchedSegId = StringUtil::fromString<segmentid_t>(st3[j]);
                GenerateOnePatch(storedSegId, patchedSegId,
                        docCounts, answer);
            }
        }
    }

    void GenerateOnePatch(segmentid_t storedSegId, segmentid_t patchedSegId,
                          const vector<uint32_t>& docCounts,
                          vector<vector<uint32_t> >& answer)
    {
        uint32_t updateCount = rand() % 10 + 5;
        map<docid_t, uint32_t> updateMap;
        docid_t maxDocCount = docCounts[patchedSegId];
        for (uint32_t i = 0; i < updateCount; ++i)
        {
            docid_t updatedId = rand() % maxDocCount;
            uint32_t value = rand() % 1000;
            updateMap[updatedId] = value;
            answer[patchedSegId][updatedId] = value;
        }
        
        string segmentDir, attrPath;
        GetMergeDestAttrPath(storedSegId, segmentDir, attrPath);
        stringstream ss;
        ss << attrPath << mAttrConfig->GetAttrName() << "/" << 
            storedSegId << "_" << patchedSegId << "." << ATTRIBUTE_PATCH_FILE_NAME;
        unique_ptr<fs::File> file(fs::FileSystem::openFile(ss.str().c_str(), WRITE));                 

        map<docid_t, uint32_t>::const_iterator it = updateMap.begin();
        for ( ; it != updateMap.end(); ++it)
        {
            file->write((void*)(&(it->first)), sizeof(docid_t));
            file->write((void*)(&(it->second)), sizeof(uint32_t));
        }
        file->close();
    }

    SegmentMergeInfos CreateSegmentMergeInfos(const vector<uint32_t>& docCounts, 
            const vector<set<docid_t> >& delDocIdSets,
            const string& mergeString)
    {
        vector<segmentid_t> mergeSegIdVect;
        GetMergedSegmentFromString(mergeString, mergeSegIdVect);

        SegmentMergeInfos segMergeInfos;
        docid_t baseDocId = 0;
        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            vector<segmentid_t>::const_iterator it = find(mergeSegIdVect.begin(),
                    mergeSegIdVect.end(), (segmentid_t)i);
            if (it != mergeSegIdVect.end())
            {
                SegmentMergeInfo mergeInfo;
                mergeInfo.segmentId = i;
                mergeInfo.segmentInfo.docCount = docCounts[i];
                mergeInfo.deletedDocCount = delDocIdSets[i].size();
                mergeInfo.baseDocId = baseDocId;
                segMergeInfos.push_back(mergeInfo);
            }
            baseDocId += docCounts[i];
        }
        return segMergeInfos;
    }

    Version CreateMergedVersion(segmentid_t maxSegmentId, 
                                const string& mergeString)
    {
        vector<segmentid_t> mergeSegmentIdVect;
        GetMergedSegmentFromString(mergeString, mergeSegmentIdVect);

        Version version;
        for (segmentid_t i = 0; i <= maxSegmentId; i++)
        {
            vector<segmentid_t>::const_iterator it = find(mergeSegmentIdVect.begin(),
                    mergeSegmentIdVect.end(), i);
            if (it == mergeSegmentIdVect.end())
            {
                version.AddSegment(i);
            }
        }

        return version;
    }

    void GetMergedSegmentFromString(const string& mergeString, 
                                    vector<segmentid_t>& segmentIdVect)
    {
        StringTokenizer st;
        st.tokenize(mergeString, ",");
        for (size_t i = 0; i < st.getNumTokens(); ++i)
        {
            segmentid_t segId = StringUtil::fromString<segmentid_t>(st[i]);
            segmentIdVect.push_back(segId);
        }
    }

    ReclaimMapPtr
    CreateReclaimMapForUpdateCase(const vector<uint32_t> &docCounts,
                                  const vector<set<docid_t> > &delDocIdSets,
                                  const string &mergeString,
                                  vector<docid_t> baseDocIds = {}) {
        vector<segmentid_t> mergeSegIdVect;
        GetMergedSegmentFromString(mergeString, mergeSegIdVect);
        return IndexTestUtil::CreateReclaimMap(docCounts, delDocIdSets, mergeSegIdVect, baseDocIds);
    }

    void CreateGlobalIDMap(const vector<uint32_t>& docCounts, 
                           const vector<set<docid_t> >& delDocIdSets,
                           const string& mergeString,
                           map<docid_t, docid_t>& globalIdMap)
    {
        vector<segmentid_t> mergeSegIdVect;
        GetMergedSegmentFromString(mergeString, mergeSegIdVect);

        docid_t newBaseId = 0;
        size_t oldSegCount = docCounts.size();
        vector<bool> isSegMergedVect;
        isSegMergedVect.resize(oldSegCount);

        vector<docid_t> newBaseIdVect;
        newBaseIdVect.resize(oldSegCount);

        size_t i = 0;
        for ( ; i < docCounts.size(); ++i)
        {
            vector<segmentid_t>::const_iterator it;
            it = find(mergeSegIdVect.begin(), mergeSegIdVect.end(), (segmentid_t)i);
            if (it == mergeSegIdVect.end()) // isn't a merged segment
            {
                newBaseIdVect[i] = newBaseId;
                newBaseId += docCounts[i];
                isSegMergedVect[i] = false;
            }
            else 
            {
                newBaseIdVect[i] = INVALID_DOCID;
                isSegMergedVect[i] = true;
            }
        }

        docid_t newGlobalIdForMergedSegs = newBaseId;
        docid_t oldBaseId = 0;
        docid_t newGlobalId = 0;

        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            set<docid_t> delDocIdUsedInMerge;
            if (!isSegMergedVect[i])
            {
                newGlobalId = newBaseIdVect[i];
            } 
            else 
            {
                newGlobalId = newGlobalIdForMergedSegs;
                delDocIdUsedInMerge = delDocIdSets[i];
            }

            docid_t oldGlobalId = oldBaseId;
            for (size_t j = 0; j < docCounts[i]; ++j)
            {
                set<docid_t>::const_iterator it = 
                    delDocIdUsedInMerge.find(j);
                if (it == delDocIdUsedInMerge.end())
                {
                    globalIdMap[oldGlobalId] = newGlobalId;
                    newGlobalId++;
                }
                else
                {
                    globalIdMap[oldGlobalId] = INVALID_DOCID;
                }
                oldGlobalId++;
            }

            oldBaseId += docCounts[i];
            if (isSegMergedVect[i])
            {
                newGlobalIdForMergedSegs = newGlobalId;
            }
        }
    }

private:
    string mDir;
    AttributeConfigPtr mAttrConfig;
    autil::mem_pool::Pool mPool;
};

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForMergeWithNoDeletedDoc);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForMergeWithDeletedDocs);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForSortMergeWithNoDeleteDoc);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForSortMergeWithDeleteDocs);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForMergeWithNoDeletedDocAndCompressData);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForMergeWithDeletedDocsAndCompressData);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForSortMergeWithNoDeleteDocAndCompressData);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForSortMergeWithDeleteDocsAndCompressData);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForMergeAllWithUpdate);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForMergePartialWithUpdate);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForMergePartialWithUpdateAndPatchMerge);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMergerTest, TestCaseForPerformanceOfMergePartialWithUpdate);


IE_NAMESPACE_END(index);
