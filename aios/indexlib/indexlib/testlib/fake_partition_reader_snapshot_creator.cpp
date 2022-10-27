#include "indexlib/testlib/fake_partition_reader_snapshot_creator.h"
#include "indexlib/testlib/fake_partition_reader_snapshot.h"
#include "indexlib/testlib/fake_index_partition_reader.h"
#include "indexlib/testlib/fake_index_reader.h"
#include "indexlib/testlib/fake_bitmap_index_reader.h"
#include "indexlib/testlib/fake_summary_reader.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(testlib);
IE_LOG_SETUP(testlib, FakePartitionReaderSnapshotCreator);
IE_LOG_SETUP(testlib, FakeIndexReader);

FakePartitionReaderSnapshotCreator::FakePartitionReaderSnapshotCreator()
{
}

FakePartitionReaderSnapshotCreator::~FakePartitionReaderSnapshotCreator()
{
}

PartitionReaderSnapshotPtr FakePartitionReaderSnapshotCreator::createIndexPartitionReader(
        const vector<FakeIndex> &fakeIndexs)
{
    vector<FakeIndexPartitionReader*> indexPartReaders;
    vector<IndexPartitionReaderInfo> indexPartReaderInfos;
    TableMem2IdMap *indexName2IdMap = new TableMem2IdMap();
    TableMem2IdMap *attrName2IdMap = new TableMem2IdMap();
    TableMem2IdMap *packAttrName2IdMap = new TableMem2IdMap();

    for (const auto &fakeIndex : fakeIndexs) {
        innerCreateIndexPartitionReader(fakeIndex, indexName2IdMap, attrName2IdMap, packAttrName2IdMap,
                indexPartReaders, indexPartReaderInfos);
    }
    return PartitionReaderSnapshotPtr(new FakePartitionReaderSnapshot(
                    indexName2IdMap, attrName2IdMap, packAttrName2IdMap, indexPartReaderInfos));
}

PartitionReaderSnapshotPtr FakePartitionReaderSnapshotCreator::createIndexPartitionReader(
        const FakeIndex &fakeIndex)
{
    vector<FakeIndexPartitionReader*> indexPartReaders;
    vector<IndexPartitionReaderInfo> indexPartReaderInfos;
    TableMem2IdMap *indexName2IdMap = new TableMem2IdMap();
    TableMem2IdMap *attrName2IdMap = new TableMem2IdMap();
    TableMem2IdMap *packAttrName2IdMap = new TableMem2IdMap();

    innerCreateIndexPartitionReader(fakeIndex, indexName2IdMap, attrName2IdMap, packAttrName2IdMap,
                                    indexPartReaders, indexPartReaderInfos);
    return PartitionReaderSnapshotPtr(new FakePartitionReaderSnapshot(
                    indexName2IdMap, attrName2IdMap, packAttrName2IdMap, indexPartReaderInfos));
}

void FakePartitionReaderSnapshotCreator::innerCreateIndexPartitionReader(
        const FakeIndex &fakeIndex,
        TableMem2IdMap *indexName2IdMap,
        TableMem2IdMap *attrName2IdMap,
        TableMem2IdMap *packAttrName2IdMap,
        vector<FakeIndexPartitionReader*> &indexPartReaders,
        vector<IndexPartitionReaderInfo> &indexPartReaderInfos)

{
    if (!fakeIndex.rootPath.empty()) {
        FileSystemWrapper::DeleteIfExist(fakeIndex.rootPath);
        FileSystemWrapper::MkDir(fakeIndex.rootPath, true);
    }

    FakeIndexPartitionReader *fakePartitionReader = new FakeIndexPartitionReader;
    fakePartitionReader->SetVersion(Version(fakeIndex.versionId));
    fakePartitionReader->SetSummaryReader(SummaryReaderPtr(
                    createFakeSummaryReader(fakeIndex.summarys)));

    initSegmentDocCounts(fakePartitionReader, fakeIndex.segmentDocCounts);

    // create deletion map reader
    DeletionMapReaderPtr delReader = createDeletionMapReader(
            fakeIndex.deletedDocIds, fakePartitionReader->getDocCount());
    fakePartitionReader->SetDeletionMapReader(delReader);

    size_t idxBegin = indexPartReaders.size();
    map<string, uint32_t> prefixToIdx;
    vector<string> tableNames;
    const vector<string> &tablePrefix = fakeIndex.tablePrefix;
    indexPartReaders.push_back(fakePartitionReader);
    tableNames.push_back(fakeIndex.tableName);
    for (size_t i = 0; i < tablePrefix.size(); ++i) {
        prefixToIdx[tablePrefix[i]] = indexPartReaders.size();
        indexPartReaders.push_back(new FakeIndexPartitionReader);
        tableNames.push_back(tablePrefix[i] + fakeIndex.tableName);
    }

    initIndexPartReaders(fakeIndex, prefixToIdx, indexPartReaders,
                         *indexName2IdMap, *attrName2IdMap, idxBegin);

    for (size_t i = idxBegin; i < indexPartReaders.size(); ++i) {
        int32_t localIdx = i - idxBegin;
        IndexPartitionReaderInfo info;
        info.tableName = tableNames[localIdx];
        info.indexPartReader.reset(indexPartReaders[i]);
        if (localIdx == 0) {
            info.readerType = READER_PRIMARY_MAIN_TYPE;
        } else if (tablePrefix[localIdx-1].find("sub_") == 0) {
            info.readerType = READER_PRIMARY_SUB_TYPE;
        } else {
            info.readerType = READER_PRIMARY_MAIN_TYPE;;
        }
        indexPartReaderInfos.push_back(info);
    }
}

DeletionMapReaderPtr FakePartitionReaderSnapshotCreator::createDeletionMapReader(
        const string &deletedDocIdStr, uint32_t totalDocCount)
{
    vector<docid_t> deletedDocIds;
    StringUtil::fromString(deletedDocIdStr, deletedDocIds, ",");
    DeletionMapReaderPtr delReaderPtr(
            new DeletionMapReader(totalDocCount));
    for (size_t i = 0; i < deletedDocIds.size(); i++) {
        delReaderPtr->Delete(deletedDocIds[i]);
    }
    return delReaderPtr;
}

void FakePartitionReaderSnapshotCreator::initSegmentDocCounts(
        FakeIndexPartitionReader *reader,
        string segDocCountsStr)
{
    if (segDocCountsStr.empty())
    {
        segDocCountsStr = "10000";
    }
    vector<string> segDocCounts = StringTokenizer::tokenize(ConstString(segDocCountsStr), "|",
            StringTokenizer::TOKEN_TRIM);
    assert(segDocCounts.size() == 1 || segDocCounts.size() == 2);
    vector<string> orderedSegDocCounts = StringTokenizer::tokenize(ConstString(segDocCounts[0]), ",",
            StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);

    vector<docid_t> baseDocIds;
    DocIdRangeVector orderedRanges;
    uint32_t segmentCount = orderedSegDocCounts.size();
    docid_t baseDocId = 0;
    for (size_t i = 0; i < orderedSegDocCounts.size(); ++i) {
        baseDocIds.push_back(baseDocId);
        uint32_t docCount = StringUtil::fromString<uint32_t>(orderedSegDocCounts[i].c_str());
        orderedRanges.push_back(DocIdRange(baseDocId, baseDocId + docCount));
        baseDocId += docCount;
    }

    reader->setIncDocCount(baseDocId);
    reader->SetOrderedDocIds(orderedRanges);
    if (segDocCounts.size() == 2) {
        baseDocIds.push_back(baseDocId);
        uint32_t docCount = StringUtil::fromString<uint32_t>(segDocCounts[1]);
        reader->SetUnOrderedDocIdRange(DocIdRange(baseDocId, baseDocId + docCount));
        baseDocId += docCount;
        segmentCount++;
    }
    reader->SetBaseDocIds(baseDocIds);
    reader->setDocCount(baseDocId);
    reader->setSegmentCount(segmentCount);
}



FakeSummaryReader *FakePartitionReaderSnapshotCreator::createFakeSummaryReader(
        const string &summaryStr)
{
    vector<vector<string> > summaryDocFields;
    StringUtil::fromString(summaryStr, summaryDocFields, ",", ";");
    FakeSummaryReader *fakeSummaryReader = new FakeSummaryReader;
    for (size_t i = 0; i < summaryDocFields.size(); i++) {
        document::SearchSummaryDocument *doc =
            new document::SearchSummaryDocument(NULL, summaryDocFields[i].size());
        for (size_t j = 0; j < summaryDocFields[i].size(); j++) {
            doc->SetFieldValue((int32_t)j, summaryDocFields[i][j].c_str(),
                    summaryDocFields[i][j].size());
        }
        fakeSummaryReader->AddSummaryDocument(doc);
    }
    return fakeSummaryReader;
}

void FakePartitionReaderSnapshotCreator::initIndexPartReaders(
        const FakeIndex &fakeIndex,
        const map<string, uint32_t> &prefixToIdx,
        const vector<FakeIndexPartitionReader*> &indexPartVec,
        TableMem2IdMap &indexName2IdMap,
        TableMem2IdMap &attrName2IdMap,
        size_t idxBegin)
{
    // do not support index right now.
    vector<FakeIndexReader *> indexReaders;
    for (size_t i = idxBegin; i < indexPartVec.size(); ++i) {
        indexReaders.push_back(new FakeIndexReader);
        indexPartVec[i]->SetIndexReader(IndexReaderPtr(indexReaders[i - idxBegin]));
    }
    for (std::map<std::string, std::string>::const_iterator it = fakeIndex.indexes.begin();
         it != fakeIndex.indexes.end(); ++it)
    {
        const string &indexName = it->first;
        string identifier;
        uint32_t idx = getIndexPartReaderIdx(prefixToIdx, indexName, identifier, idxBegin);
        indexName2IdMap[make_pair(fakeIndex.tableName, indexName)] = idx;
        if (identifier == "pk") {
            indexPartVec[idx]->createFakePrimaryKeyReader<uint64_t>(
                    "", fakeIndex.rootPath, it->second, pk_default_hash);
        } else if (identifier == "number_pk") {
            indexPartVec[idx]->createFakePrimaryKeyReader<uint64_t>(
                    "", fakeIndex.rootPath, it->second, pk_number_hash);
        } else if (identifier.length() >= 6 && identifier.substr(0, 6) == "bitmap") {
            indexReaders[idx - idxBegin]->addIndexReader(indexName, new FakeBitmapIndexReader(it->second));
        } else {
            indexReaders[idx - idxBegin]->addIndexReader(indexName, new FakeTextIndexReader(it->second));
        }
    }

    // create attribute reader
    vector<string> attributes = StringTokenizer::tokenize(ConstString(fakeIndex.attributes),
            ";", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < attributes.size(); ++i) {
        vector<string> attrItems = StringTokenizer::tokenize(ConstString(attributes[i]),
                fakeIndex.fieldSep, StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        assert(attrItems.size() == 3);
        string identifier;
        uint32_t idx = getIndexPartReaderIdx(prefixToIdx, attrItems[0], identifier, idxBegin);
        string attrName = attrItems[0];
        //remove prefix of sub join cache
        if (idx - idxBegin == 1) {
            size_t pos = attrItems[0].find("_@_subjoin_docid_");
            if (pos != string::npos) {
                attrName = attrItems[0].substr(pos);
            }
        }
        attrName2IdMap[make_pair(fakeIndex.tableName, attrName)] = idx;
        createFakeAttributeReader(indexPartVec[idx], attrName, attrItems[1], attrItems[2],
                fakeIndex.rootPath, fakeIndex.stringValueSep);
    }
}


uint32_t FakePartitionReaderSnapshotCreator::getIndexPartReaderIdx(
        const map<string, uint32_t> &prefixToIdx,
        const string &orgIdentifier, string &newIdentifier, uint32_t defaultIdx)
{
    for (map<string, uint32_t>::const_iterator it = prefixToIdx.begin();
         it != prefixToIdx.end(); ++it)
    {
        if (orgIdentifier.find(it->first) == 0) {
            newIdentifier = orgIdentifier.substr(it->first.size());
            return it->second;
        }
    }
    newIdentifier = orgIdentifier;
    return defaultIdx;
}

void FakePartitionReaderSnapshotCreator::createFakeAttributeReader(
        FakeIndexPartitionReader *fakePartitionReader,
        const string &attrName, const string &type,
        const string &values,
        const string &rootPath,
        const string &stringValueSep)
{
    if (attrName == "main_docid_to_sub_docid_attr_name"
        || attrName == "sub_docid_to_main_docid_attr_name")
    {
        assert(type == "int32_t");
        fakePartitionReader->createFakeJoinDocidAttributeReader(attrName, values);
        return;
    }

    if (type == "int8_t") {
        fakePartitionReader->createFakeAttributeReader<int8_t>(attrName, values);
    } else if (type == "uint8_t") {
        fakePartitionReader->createFakeAttributeReader<uint8_t>(attrName, values);
    } else if (type == "int16_t") {
        fakePartitionReader->createFakeAttributeReader<int16_t>(attrName, values);
    } else if (type == "uint16_t") {
        fakePartitionReader->createFakeAttributeReader<uint16_t>(attrName, values);
    } else if (type == "int32_t") {
        fakePartitionReader->createFakeAttributeReader<int32_t>(attrName, values);
    } else if (type == "uint32_t") {
        fakePartitionReader->createFakeAttributeReader<uint32_t>(attrName, values);
    } else if (type == "int64_t") {
        fakePartitionReader->createFakeAttributeReader<int64_t>(attrName, values);
    } else if (type == "uint64_t") {
        fakePartitionReader->createFakeAttributeReader<uint64_t>(attrName, values);
    } else if (type == "float") {
        fakePartitionReader->createFakeAttributeReader<float>(attrName, values);
    } else if (type == "double") {
        fakePartitionReader->createFakeAttributeReader<double>(attrName, values);
    } else if (type == "string") {
        fakePartitionReader->createFakeStringAttributeReader(attrName, values, rootPath, stringValueSep);
    } else if (type == "multi_int8_t") {
        fakePartitionReader->createFakeMultiAttributeReader<int8_t>(attrName, values, rootPath);
    } else if (type == "multi_uint8_t") {
        fakePartitionReader->createFakeMultiAttributeReader<uint8_t>(attrName, values, rootPath);
    } else if (type == "multi_int16_t") {
        fakePartitionReader->createFakeMultiAttributeReader<int16_t>(attrName, values, rootPath);
    } else if (type == "multi_uint16_t") {
        fakePartitionReader->createFakeMultiAttributeReader<uint16_t>(attrName, values, rootPath);
    } else if (type == "multi_int32_t") {
        fakePartitionReader->createFakeMultiAttributeReader<int32_t>(attrName, values, rootPath);
    } else if (type == "multi_uint32_t") {
        fakePartitionReader->createFakeMultiAttributeReader<uint32_t>(attrName, values, rootPath);
    } else if (type == "multi_int64_t") {
        fakePartitionReader->createFakeMultiAttributeReader<int64_t>(attrName, values, rootPath);
    } else if (type == "multi_uint64_t") {
        fakePartitionReader->createFakeMultiAttributeReader<uint64_t>(attrName, values, rootPath);
    } else if (type == "multi_float") {
        fakePartitionReader->createFakeMultiAttributeReader<float>(attrName, values, rootPath);
    } else if (type == "multi_double") {
        fakePartitionReader->createFakeMultiAttributeReader<double>(attrName, values, rootPath);
    } else if (type == "multi_string") {
        fakePartitionReader->createFakeMultiAttributeReader<MultiChar>(attrName, values, rootPath);
    }
}


IE_NAMESPACE_END(testlib);
