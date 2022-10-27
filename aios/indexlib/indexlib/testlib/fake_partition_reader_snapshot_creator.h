#ifndef __INDEXLIB_FAKEPARTITIONREADERSNAPSHOTCREATOR_H
#define __INDEXLIB_FAKEPARTITIONREADERSNAPSHOTCREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/partition_reader_snapshot.h"

IE_NAMESPACE_BEGIN(testlib);

struct FakeIndex {
    FakeIndex() {
        versionId = 0;
        stringValueSep = ",";
        fieldSep = ":";
        tableName = "table";
    }
    ~FakeIndex() {
    }
    std::string rootPath;

    std::map<std::string, std::string> indexes;

    // summary format:
    //    field1,field2,field3,...,fieldN;   ---> doc1
    //    field1,field2,field3,...,fieldN;   ---> doc2
    std::string summarys;
    // attribute format:
    //    attrName:attrType:doc1,doc2,...,docN;
    //    attrName:attrType:doc1,doc2,...,docN;
    std::string attributes;
    // const attribute format:
    //    attrName:attrType;
    //    attrName:attrType;
    std::string constAttributes;
    // deletion map format:
    //    doc1,doc2,...,docN
    std::string deletedDocIds;
    std::string partitionMeta;
    // segmentDocCount1,segmentDocCount2|rtSegmentDocCount
    std::string segmentDocCounts;
    std::string stringValueSep;
    std::string fieldSep;
    versionid_t versionId;
    uint32_t totalDocCount;
    std::string tableName;
    std::vector<std::string> tablePrefix;

    void clear() {
        indexes.clear();
        summarys.clear();
        attributes.clear();
        deletedDocIds.clear();
        partitionMeta.clear();
        versionId = 0;
        tablePrefix.clear();
    }
};
class FakeSummaryReader;
class FakeIndexPartitionReader;
class FakePartitionReaderSnapshotCreator
{
public:
    FakePartitionReaderSnapshotCreator();
    ~FakePartitionReaderSnapshotCreator();
public:
    static partition::PartitionReaderSnapshotPtr createIndexPartitionReader(
            const FakeIndex &fakeIndex);
    static partition::PartitionReaderSnapshotPtr createIndexPartitionReader(
            const std::vector<FakeIndex> &fakeIndexs);
private:
    static void innerCreateIndexPartitionReader(
        const FakeIndex &fakeIndex,
        partition::TableMem2IdMap *indexName2IdMap,
        partition::TableMem2IdMap *attrName2IdMap,
        partition::TableMem2IdMap *packAttrName2IdMap,
        std::vector<FakeIndexPartitionReader*> &indexPartReaders,
        std::vector<partition::IndexPartitionReaderInfo> &indexPartReaderInfos);
    static FakeSummaryReader *createFakeSummaryReader(const std::string &summaryStr);
    static void initIndexPartReaders(const FakeIndex &fakeIndex,
                                     const std::map<std::string, uint32_t> &prefixToIdx,
                                     const std::vector<FakeIndexPartitionReader*> &indexPartVec,
                                     partition::TableMem2IdMap &indexName2IdMap,
                                     partition::TableMem2IdMap &attrName2IdMap,
            size_t idxBegin);
    static uint32_t getIndexPartReaderIdx(const std::map<std::string, uint32_t> &prefixToIdx,
            const std::string &orgIdentifier, std::string &newIdentifier, uint32_t defaultIdx);
    static void createFakeAttributeReader(FakeIndexPartitionReader *fakePartitionReader,
            const std::string &attrName, const std::string &type,
            const std::string &values, const std::string &rootPath, 
            const std::string &stringValueSep);
    static void initSegmentDocCounts(FakeIndexPartitionReader *reader,
            std::string segDocCountsStr);
    static index::DeletionMapReaderPtr createDeletionMapReader(
            const std::string &deletedDocIdStr, uint32_t totalDocCount);
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakePartitionReaderSnapshotCreator);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKEPARTITIONREADERSNAPSHOTCREATOR_H
