#include "indexlib/index/test/index_test_util.h"

#include "autil/StringUtil.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/common/field_format/section_attribute/section_attribute_formatter.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index/test/posting_maker.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/partition/on_disk_partition_data.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::config;

using namespace indexlib::file_system;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::merger;

namespace indexlib { namespace index {

IndexTestUtil::ToDelete IndexTestUtil::deleteFuncs[IndexTestUtil::DM_MAX_MODE] = {
    IndexTestUtil::NoDelete,   IndexTestUtil::DeleteEven, IndexTestUtil::DeleteSome,
    IndexTestUtil::DeleteMany, IndexTestUtil::DeleteAll,
};

DeletionMapReaderPtr IndexTestUtil::CreateDeletionMap(const Version& version, const vector<uint32_t>& docCounts,
                                                      IndexTestUtil::ToDelete toDel)
{
    DeletionMapReaderPtr deletionMapReader = InnerCreateDeletionMapReader(version, docCounts);

    docid_t id = 0;
    for (size_t i = 0; i < docCounts.size(); ++i) {
        for (uint32_t j = 0; j < docCounts[i]; ++j) {
            if (toDel(id)) {
                deletionMapReader->Delete(id);
            }
            ++id;
        }
    }
    return deletionMapReader;
}

DeletionMapReaderPtr IndexTestUtil::CreateDeletionMap(const merger::SegmentDirectoryPtr& segDir,
                                                      const std::vector<uint32_t>& docCounts,
                                                      const vector<set<docid_t>>& delDocIdSets)
{
    DeletionMapReaderPtr deletionMapReader = InnerCreateDeletionMapReader(segDir->GetVersion(), docCounts);

    docid_t baseDocId = 0;
    for (size_t i = 0; i < docCounts.size(); ++i) {
        for (docid_t docId = 0; docId < (docid_t)docCounts[i]; docId++) {
            docid_t globalId = docId + baseDocId;
            set<docid_t>::const_iterator it = delDocIdSets[i].find(globalId);
            if (it != delDocIdSets[i].end()) {
                deletionMapReader->Delete(globalId);
            }
        }
        baseDocId += docCounts[i];
    }

    return deletionMapReader;
}

DeletionMapReaderPtr IndexTestUtil::InnerCreateDeletionMapReader(const Version& version,
                                                                 const vector<uint32_t>& docCounts)
{
    assert(version.GetSegmentCount() == docCounts.size());
    DeletionMapReader::SegmentMetaMap segmentMetaMap;
    uint32_t totalDocCount = 0;
    for (size_t i = 0; i < version.GetSegmentCount(); ++i) {
        segmentMetaMap[version[i]] = make_pair(totalDocCount, docCounts[i]);
        totalDocCount += docCounts[i];
    }
    DeletionMapReaderPtr deletionMapReader(new DeletionMapReader(totalDocCount));
    deletionMapReader->mSegmentMetaMap = segmentMetaMap;
    return deletionMapReader;
}

void IndexTestUtil::CreateDeletionMap(segmentid_t segId, uint32_t docCount, IndexTestUtil::ToDelete toDel,
                                      DeletionMapReaderPtr& deletionMapReader)
{
    for (docid_t i = 0; i < (docid_t)docCount; ++i) {
        if (toDel(i)) {
            deletionMapReader->Delete(segId, i);
        }
    }
}

void IndexTestUtil::ResetDir(const string& dir)
{
    CleanDir(dir);
    MkDir(dir);
}

void IndexTestUtil::MkDir(const string& dir)
{
    if (!FslibWrapper::IsExist(dir).GetOrThrow()) {
        try {
            FslibWrapper::MkDirE(dir, true);
        } catch (const FileIOException&) {
            std::cout << "Create work directory: [" << dir << "] FAILED" << std::endl;
        }
    }
}

void IndexTestUtil::CleanDir(const string& dir)
{
    if (FslibWrapper::IsExist(dir).GetOrThrow()) {
        try {
            FslibWrapper::DeleteDirE(dir, DeleteOption::NoFence(false));
        } catch (const FileIOException&) {
            std::cout << "Remove directory: [" << dir << "] FAILED." << std::endl;
            assert(false);
        }
    }
}

Version IndexTestUtil::CreateVersion(uint32_t segCount, segmentid_t baseSegId, const file_system::DirectoryPtr& dir)
{
    Version version;
    for (uint32_t i = 0; i < segCount; i++) {
        auto segId = i + baseSegId;
        version.AddSegment(segId);
        if (dir != nullptr) {
            dir->MakeDirectory(version.GetSegmentDirName(segId));
        }
    }
    return version;
}

merger::SegmentDirectoryPtr IndexTestUtil::CreateSegmentDirectory(const file_system::DirectoryPtr& dir,
                                                                  uint32_t segCount, segmentid_t baseSegId)
{
    Version version = CreateVersion(segCount, baseSegId, dir);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(dir, version));
    return segDir;
}

merger::SegmentDirectoryPtr IndexTestUtil::CreateSegmentDirectory(const file_system::DirectoryPtr& dir, size_t segCount,
                                                                  segmentid_t baseSegId, versionid_t vid)
{
    Version version(vid);
    for (size_t i = 0; i < segCount; i++) {
        version.AddSegment(i + baseSegId);
    }

    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(dir, version));
    return segDir;
}

/////////////////////////////////////////////////////////////
class FakeReclaimMap : public ReclaimMap
{
private:
    using ReclaimMap::Init;

public:
    void Init(const vector<uint32_t>& docCounts, const vector<set<docid_t>>& delDocIdSets, bool needReverse)
    {
        uint32_t docCount = 0;
        for (size_t i = 0; i < docCounts.size(); ++i) {
            docCount += docCounts[i];
        }
        mDocIdArray.resize(docCount);
        docid_t id = 0;
        for (size_t i = 0; i < docCounts.size(); ++i) {
            for (size_t j = 0; j < docCounts[i]; ++j) {
                set<docid_t>::const_iterator it = delDocIdSets[i].find(j);
                if (it == delDocIdSets[i].end()) {
                    mDocIdArray[id] = mNewDocId++;
                } else {
                    mDocIdArray[id] = INVALID_DOCID;
                    mDeletedDocCount++;
                }
                id++;
            }
        }
        if (needReverse) {
            // prepare segment merge info
            SegmentMergeInfos segMergeInfos;
            docid_t baseDocId = 0;
            for (size_t i = 0; i < docCounts.size(); i++) {
                segmentid_t segId = (segmentid_t)i;
                uint32_t docCount = docCounts[i];
                index_base::SegmentInfo segInfo;
                segInfo.docCount = docCount;
                SegmentMergeInfo segMergeInfo(segId, segInfo, delDocIdSets[i].size(), baseDocId);
                baseDocId += docCount;
                segMergeInfos.push_back(segMergeInfo);
            }
            InitReverseDocIdArray(segMergeInfos);
        }
    }

    void Init(const vector<uint32_t>& docCounts, const vector<set<docid_t>>& delDocIdSets,
              const vector<segmentid_t>& mergeSegIdVect)
    {
        uint32_t docCount = 0;
        for (size_t i = 0; i < docCounts.size(); ++i) {
            docCount += docCounts[i];
        }
        mDocIdArray.resize(docCount);

        docid_t newLocalId = 0;
        docid_t oldBaseId = 0;

        for (size_t i = 0; i < docCounts.size(); ++i) {
            vector<segmentid_t>::const_iterator it;
            it = find(mergeSegIdVect.begin(), mergeSegIdVect.end(), (segmentid_t)i);
            if (it == mergeSegIdVect.end()) {
                oldBaseId += docCounts[i];
                continue;
            }

            docid_t oldGlobalId = oldBaseId;
            for (size_t j = 0; j < docCounts[i]; ++j) {
                set<docid_t>::const_iterator it = delDocIdSets[i].find(j);
                if (it == delDocIdSets[i].end()) {
                    mDocIdArray[oldGlobalId] = newLocalId;
                    newLocalId++;
                } else {
                    mDocIdArray[oldGlobalId] = INVALID_DOCID;
                    mDeletedDocCount++;
                }
                oldGlobalId++;
            }
            oldBaseId += docCounts[i];
        }
    }

    void SetTargetBaseDocIds(std::vector<docid_t> targetBaseDocIds) { mTargetBaseDocIds = std::move(targetBaseDocIds); }
};

class SortMergeReclaimMap : public ReclaimMap
{
private:
    using ReclaimMap::Init;

public:
    void Init(const vector<uint32_t>& docCounts, const vector<set<docid_t>>& delDocIdSets)
    {
        uint32_t docCount = 0;
        for (size_t i = 0; i < docCounts.size(); ++i) {
            docCount += docCounts[i];
        }

        mDocIdArray.resize(docCount);

        vector<docid_t> baseDocIds;
        docid_t baseDocId = 0;
        for (size_t i = 0; i < docCounts.size(); ++i) {
            baseDocIds.push_back(baseDocId);
            baseDocId += docCounts[i];
        }

        docid_t id = 0;
        for (size_t i = 0; i < docCounts.size(); ++i) {
            for (size_t j = 0; j < docCounts[i] / 2; ++j) {
                set<docid_t>::const_iterator it = delDocIdSets[i].find(j);
                if (it == delDocIdSets[i].end()) {
                    mDocIdArray[j + baseDocIds[i]] = id;
                    id++;
                } else {
                    mDocIdArray[j + baseDocIds[i]] = INVALID_DOCID;
                    mDeletedDocCount++;
                }
            }
        }

        for (size_t i = 0; i < docCounts.size(); ++i) {
            for (size_t j = docCounts[i] / 2; j < docCounts[i]; ++j) {
                set<docid_t>::const_iterator it = delDocIdSets[i].find(j);
                if (it == delDocIdSets[i].end()) {
                    mDocIdArray[j + baseDocIds[i]] = id;
                    id++;
                } else {
                    mDocIdArray[j + baseDocIds[i]] = INVALID_DOCID;
                    mDeletedDocCount++;
                }
            }
        }
        mNewDocId = id;
    }
    void SetTargetBaseDocIds(std::vector<docid_t> targetBaseDocIds) { mTargetBaseDocIds = std::move(targetBaseDocIds); }
};

uint32_t IndexTestUtil::GetTotalDocCount(const vector<uint32_t>& docCounts, const vector<set<docid_t>>& delDocIdSets,
                                         const SegmentMergeInfos& segMergeInfos)
{
    uint32_t total = 0;
    if (segMergeInfos.empty()) {
        for (size_t i = 0; i < docCounts.size(); ++i) {
            total += docCounts[i] - delDocIdSets[i].size();
        }
        return total;
    }

    vector<uint32_t> mergeDocCounts;
    vector<set<docid_t>> mergeDelDocIdSets;
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        segmentid_t segId = segMergeInfos[i].segmentId;
        mergeDocCounts.push_back(docCounts[segId]);
        mergeDelDocIdSets.push_back(delDocIdSets[segId]);
    }
    for (size_t i = 0; i < mergeDocCounts.size(); ++i) {
        total += mergeDocCounts[i] - mergeDelDocIdSets[i].size();
    }
    return total;
}

vector<docid_t> IndexTestUtil::GetTargetBaseDocIds(const vector<uint32_t>& docCounts,
                                                   const vector<set<docid_t>>& delDocIdSets,
                                                   SegmentMergeInfos& segMergeInfos, uint32_t targetSegmentCount)
{
    vector<docid_t> result;
    if (targetSegmentCount < 2) {
        return result;
    }
    uint32_t total = GetTotalDocCount(docCounts, delDocIdSets, segMergeInfos);
    size_t splitFactor = total / targetSegmentCount;
    result.resize(targetSegmentCount);
    for (uint32_t i = 0; i < targetSegmentCount; ++i) {
        result[i] = i * splitFactor;
    }
    return result;
}

ReclaimMapPtr IndexTestUtil::CreateReclaimMap(const vector<uint32_t>& docCounts,
                                              const vector<set<docid_t>>& delDocIdSets, bool needReverse,
                                              const std::vector<docid_t>& targetBaseDocIds)
{
    FakeReclaimMap* fakeReclaimMap = new FakeReclaimMap();
    fakeReclaimMap->Init(docCounts, delDocIdSets, needReverse);
    fakeReclaimMap->SetTargetBaseDocIds(targetBaseDocIds);
    ReclaimMapPtr reclaimMap(fakeReclaimMap);
    return reclaimMap;
}

ReclaimMapPtr IndexTestUtil::CreateReclaimMap(const vector<uint32_t>& docCounts,
                                              const vector<vector<docid_t>>& delDocIds,
                                              const std::vector<docid_t>& targetBaseDocIds)
{
    vector<set<docid_t>> delDocIdSets;
    for (size_t i = 0; i < delDocIds.size(); i++) {
        set<docid_t> docIdSet;
        docIdSet.insert(delDocIds[i].begin(), delDocIds[i].end());
        delDocIdSets.push_back(docIdSet);
    }
    return IndexTestUtil::CreateReclaimMap(docCounts, delDocIdSets, targetBaseDocIds);
}

ReclaimMapPtr IndexTestUtil::CreateReclaimMap(const string& segmentDocIdDesc,
                                              const std::vector<docid_t>& targetBaseDocIds)
{
    vector<string> segmentStrVec = StringUtil::split(segmentDocIdDesc, ";");
    vector<uint32_t> docCounts;
    vector<vector<docid_t>> delDocIds;
    for (size_t i = 0; i < segmentStrVec.size(); i++) {
        vector<string> docIdsStr = StringUtil::split(segmentStrVec[i], ":");
        assert(docIdsStr.size() > 0);
        docCounts.push_back(StringUtil::fromString<uint32_t>(docIdsStr[0]));
        vector<docid_t> segmentDelDocIds;
        if (docIdsStr.size() > 1) {
            StringUtil::fromString(docIdsStr[1], segmentDelDocIds, ",");
        }
        delDocIds.push_back(segmentDelDocIds);
    }
    return CreateReclaimMap(docCounts, delDocIds, targetBaseDocIds);
}

ReclaimMapPtr IndexTestUtil::CreateReclaimMap(const vector<uint32_t>& docCounts,
                                              const vector<set<docid_t>>& delDocIdSets,
                                              const vector<segmentid_t>& mergeSegIdVect,
                                              const std::vector<docid_t>& targetBaseDocIds)
{
    FakeReclaimMap* fakeReclaimMap = new FakeReclaimMap();
    fakeReclaimMap->Init(docCounts, delDocIdSets, mergeSegIdVect);
    fakeReclaimMap->SetTargetBaseDocIds(targetBaseDocIds);
    ReclaimMapPtr reclaimMap(fakeReclaimMap);
    return reclaimMap;
}

ReclaimMapPtr IndexTestUtil::CreateSortMergingReclaimMap(const vector<uint32_t>& docCounts,
                                                         const vector<vector<docid_t>>& delDocIds,
                                                         const std::vector<docid_t>& targetBaseDocIds)
{
    vector<set<docid_t>> delDocIdSets;
    for (size_t i = 0; i < delDocIds.size(); i++) {
        set<docid_t> docIdSet;
        docIdSet.insert(delDocIds[i].begin(), delDocIds[i].end());
        delDocIdSets.push_back(docIdSet);
    }
    return IndexTestUtil::CreateSortMergingReclaimMap(docCounts, delDocIdSets, targetBaseDocIds);
}

ReclaimMapPtr IndexTestUtil::CreateSortMergingReclaimMap(const vector<uint32_t>& docCounts,
                                                         const vector<set<docid_t>>& delDocIdSets,
                                                         const std::vector<docid_t>& targetBaseDocIds)
{
    SortMergeReclaimMap* sortMergeReclaimMap = new SortMergeReclaimMap();
    sortMergeReclaimMap->Init(docCounts, delDocIdSets);
    sortMergeReclaimMap->SetTargetBaseDocIds(targetBaseDocIds);
    ReclaimMapPtr reclaimMap(sortMergeReclaimMap);
    return reclaimMap;
}

DeletionMapReaderPtr IndexTestUtil::CreateDeletionMapReader(const vector<uint32_t>& docNums)
{
    Version version(0);
    for (size_t i = 0; i < docNums.size(); i++) {
        version.AddSegment((segmentid_t)i);
    }
    return InnerCreateDeletionMapReader(version, docNums);
}

void IndexTestUtil::BuildMultiSegmentsFromDataStrings(const vector<string>& dataStrs, const string& rootDir,
                                                      const vector<docid_t>& baseDocIds, vector<AnswerMap>& answerMaps,
                                                      vector<uint8_t>& compressModes,
                                                      const IndexFormatOption& indexFormatOption)
{
    for (size_t i = 0; i < dataStrs.size(); i++) {
        stringstream ss;
        ss << rootDir << SEGMENT_FILE_NAME_PREFIX << "_" << i << "_level_0/";
        string indexDir = ss.str() + INDEX_DIR_NAME + "/";
        fslib::fs::FileSystem::mkDir(indexDir, true);
        string dumpFilePath = indexDir + POSTING_FILE_NAME;

        AnswerMap tempMap;
        uint8_t compressMode =
            BuildOneSegmentFromDataString(dataStrs[i], dumpFilePath, baseDocIds[i], tempMap, indexFormatOption);
        compressModes.push_back(compressMode);
        answerMaps.push_back(tempMap);
    }
}

uint8_t IndexTestUtil::BuildOneSegmentFromDataString(const std::string& dataStr, const std::string& filePath,
                                                     docid_t baseDocId, AnswerMap& answerMap,
                                                     const IndexFormatOption& indexFormatOption)
{
    Pool byteSlicePool(SectionAttributeFormatter::DATA_SLICE_LEN * 16);
    RecyclePool bufferPool(SectionAttributeFormatter::DATA_SLICE_LEN * 16);
    SimplePool simplePool;

    PostingFormatOption formatOption = indexFormatOption.GetPostingFormatOption();

    PostingWriterResource writerResource(&simplePool, &byteSlicePool, &bufferPool, formatOption);
    PostingWriterImpl writer(&writerResource);
    answerMap = legacy::PostingMaker::MakeDocMap(dataStr);
    legacy::PostingMaker::BuildPostingData(writer, answerMap, baseDocId);

    file_system::BufferedFileWriterPtr file(new file_system::BufferedFileWriter);
    THROW_IF_FS_ERROR(file->Open(filePath, filePath), "BufferedFileWriter open failed, path [%s]", filePath.c_str());
    writer.EndSegment();

    TermMetaDumper tmDumper(formatOption);
    TermMeta termMeta(writer.GetDF(), writer.GetTotalTF(), 0);
    tmDumper.Dump(file, termMeta);
    writer.Dump(file);
    file->Close().GetOrThrow();
    return ShortListOptimizeUtil::GetCompressMode(writer.GetDF(), writer.GetTotalTF(),
                                                  formatOption.GetDocListCompressMode());
}

SegmentData IndexTestUtil::CreateSegmentData(const file_system::DirectoryPtr& directory, const SegmentInfo& segmentInfo,
                                             segmentid_t segId, docid_t baseDocId)
{
    SegmentData segmentData;
    segmentData.SetSegmentInfo(segmentInfo);
    segmentData.SetSegmentId(segId);
    segmentData.SetDirectory(directory);
    segmentData.SetBaseDocId(baseDocId);
    return segmentData;
}

index_base::PartitionDataPtr IndexTestUtil::CreatePartitionData(const file_system::IFileSystemPtr& fileSystem,
                                                                uint32_t segCount, segmentid_t baseSegId)
{
    // TODO: by default, baseSegId=0
    Version version = CreateVersion(segCount, baseSegId);
    return CreatePartitionData(fileSystem, version);
}

index_base::PartitionDataPtr IndexTestUtil::CreatePartitionData(const file_system::IFileSystemPtr& fileSystem,
                                                                Version version)
{
    return OnDiskPartitionData::CreateOnDiskPartitionData(fileSystem, version);
}
}} // namespace indexlib::index
