#pragma once

#include <map>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/PartialIndexPartitionReaderWrapper.h"
#include "ha3_sdk/testlib/index/FakeIndexParam.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace partition {
class FakeIndexPartitionReader;
} // namespace partition
} // namespace indexlib

namespace indexlib {
namespace index {

typedef isearch::common::FakeIndex FakeIndex;

class FakeIndexPartitionReaderCreator {
public:
    FakeIndexPartitionReaderCreator();
    ~FakeIndexPartitionReaderCreator();

private:
    FakeIndexPartitionReaderCreator(const FakeIndexPartitionReaderCreator &);
    FakeIndexPartitionReaderCreator &operator=(const FakeIndexPartitionReaderCreator &);

public:
    static isearch::search::IndexPartitionReaderWrapperPtr
    createIndexPartitionReader(const FakeIndex &fakeIndex,
                               indexlib::index::ErrorCode seekRet = indexlib::index::ErrorCode::OK);
    static isearch::search::PartialIndexPartitionReaderWrapperPtr
    createPartialIndexPartitionReader(const FakeIndex &fakeIndex,
                                      indexlib::index::ErrorCode seekRet
                                      = indexlib::index::ErrorCode::OK);
    static void createFakeAttributeReader(partition::FakeIndexPartitionReader *fakePartitionReader,
                                          const std::string &attrName,
                                          const std::string &type,
                                          const std::string &values);
    static void createFakeSummaryReader(partition::FakeIndexPartitionReader *fakePartitionReader,
                                        const std::string &summaryStr);
    static DeletionMapReaderPtr createDeletionMapReader(const std::string &deletedDocIdStr,
                                                        uint32_t totalDocCount);

private:
    static void
    initIndexPartReaders(const FakeIndex &fakeIndex,
                         const std::map<std::string, uint32_t> &prefixToIdx,
                         const std::vector<partition::FakeIndexPartitionReader *> &indexPartVec,
                         std::map<std::string, uint32_t> &indexName2IdMap,
                         std::map<std::string, uint32_t> &attrName2IdMap,
                         indexlib::index::ErrorCode seekRet);
    static void initSegmentDocCounts(partition::FakeIndexPartitionReader *reader,
                                     const std::string &segDocCountsStr);

    static uint32_t getIndexPartReaderIdx(const std::map<std::string, uint32_t> &prefixToIdx,
                                          const std::string &orgIdentifier,
                                          std::string &newIdentifier);
};

} // namespace index
} // namespace indexlib
