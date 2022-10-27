#ifndef ISEARCH_FAKEINDEXPARTITIONREADERCREATOR_H
#define ISEARCH_FAKEINDEXPARTITIONREADERCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/search/PartialIndexPartitionReaderWrapper.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/common/FakeIndexParam.h>

IE_NAMESPACE_BEGIN(index);

typedef HA3_NS(common)::FakeIndex FakeIndex;

class FakeIndexPartitionReaderCreator
{
public:
    FakeIndexPartitionReaderCreator();
    ~FakeIndexPartitionReaderCreator();
private:
    FakeIndexPartitionReaderCreator(const FakeIndexPartitionReaderCreator &);
    FakeIndexPartitionReaderCreator& operator=(const FakeIndexPartitionReaderCreator &);
public:
    static HA3_NS(search)::IndexPartitionReaderWrapperPtr createIndexPartitionReader(
        const FakeIndex &fakeIndex,
        IE_NAMESPACE(common)::ErrorCode seekRet = IE_NAMESPACE(common)::ErrorCode::OK);
    static HA3_NS(search)::PartialIndexPartitionReaderWrapperPtr
        createPartialIndexPartitionReader(
                const FakeIndex &fakeIndex,
                IE_NAMESPACE(common)::ErrorCode seekRet = IE_NAMESPACE(common)::ErrorCode::OK);
    static void createFakeAttributeReader(
            partition::FakeIndexPartitionReader *fakePartitionReader,
            const std::string &attrName, 
            const std::string &type, const std::string &values);
    static void createFakeSummaryReader(
            partition::FakeIndexPartitionReader *fakePartitionReader,
            const std::string &summaryStr);
    static DeletionMapReaderPtr createDeletionMapReader(
            const std::string &deletedDocIdStr, uint32_t totalDocCount);
private:
    static void initIndexPartReaders(
            const FakeIndex &fakeIndex, const std::map<std::string, uint32_t> &prefixToIdx,
            const std::vector<partition::FakeIndexPartitionReader*> &indexPartVec,
            std::map<std::string, uint32_t> &indexName2IdMap,
            std::map<std::string, uint32_t> &attrName2IdMap,
            IE_NAMESPACE(common)::ErrorCode seekRet);
    static void initSegmentDocCounts(
            partition::FakeIndexPartitionReader *reader,
            const std::string &segDocCountsStr);

    static uint32_t getIndexPartReaderIdx(
            const std::map<std::string, uint32_t> &prefixToIdx,
            const std::string &orgIdentifier, std::string &newIdentifier);
};

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEINDEXPARTITIONREADERCREATOR_H
