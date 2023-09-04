#include "indexlib/index/inverted_index/builtin_index/bitmap/test/BitmapPostingMaker.h"

#include "autil/HashAlgorithm.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexWriter.h"
#include "indexlib/util/Random.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BitmapPostingMaker);

BitmapPostingMaker::BitmapPostingMaker(const std::shared_ptr<file_system::Directory>& rootDir,
                                       const std::string& indexName, bool enableNullTerm)
    : _rootDir(rootDir)
    , _indexName(indexName)
    , _enableNullTerm(enableNullTerm)
{
    srand(8888);
}

BitmapPostingMaker::~BitmapPostingMaker() {}

uint32_t BitmapPostingMaker::RandomTokenNum() { return util::dev_urandom() % 10 + 5; }

uint32_t BitmapPostingMaker::OneTokenNum() { return 1; }

void BitmapPostingMaker::MakeOnePostingList(segmentid_t segId, uint32_t docNum, docid_t baseDocId, PostingList* answer)
{
    Key2DocIdListMap key2DocIdListMap;
    InnerMakeOneSegmentData(segId, docNum, baseDocId, &key2DocIdListMap, OneTokenNum);

    answer->first = baseDocId;
    answer->second = key2DocIdListMap.begin()->second;
}

void BitmapPostingMaker::MakeOneSegmentData(segmentid_t segId, uint32_t docNum, docid_t baseDocId,
                                            Key2DocIdListMap* answer)
{
    InnerMakeOneSegmentData(segId, docNum, baseDocId, answer, RandomTokenNum);
}
void BitmapPostingMaker::InnerMakeOneSegmentData(segmentid_t segId, uint32_t docNum, docid_t baseDocId,
                                                 Key2DocIdListMap* answer, TokenNumFunc tokenNumFunc)
{
    file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
    std::string segInfoName = std::string(SEGMENT_FILE_NAME_PREFIX) + "_" + std::to_string(segId) + "_level_0/";
    _rootDir->RemoveDirectory(segInfoName, removeOption /* may not exist */);
    auto segInfoDir = _rootDir->MakeDirectory(segInfoName);
    assert(segInfoDir != nullptr);

    // write segment info
    indexlibv2::framework::SegmentInfo segInfo;
    segInfo.docCount = docNum;
    [[maybe_unused]] auto status = segInfo.Store(segInfoDir->GetIDirectory());
    assert(status.IsOK());
    if (docNum == 0) {
        return;
    }

    autil::mem_pool::Pool* byteSlicePool = new autil::mem_pool::Pool(4 * 1024 * 1024);
    util::SimplePool simplePool;
    {
        index::BitmapIndexWriter writer(byteSlicePool, &simplePool, false);
        for (uint32_t i = 0; i < docNum; ++i) {
            uint32_t tokenNum = tokenNumFunc();
            if (_enableNullTerm && (i % 4) == 1) {
                tokenNum = 0;
            }

            std::set<std::string> keysInCurDoc;
            if (tokenNum == 0) {
                // add null term token
                writer.AddToken(index::DictKeyInfo::NULL_TERM, 0);
                keysInCurDoc.insert("__NULL__");
            } else {
                for (uint32_t j = 0; j < tokenNum; ++j) {
                    std::stringstream ssKey;
                    ssKey << "token" << (j % 7);
                    uint64_t key = autil::HashAlgorithm::hashString64(ssKey.str().c_str());

                    writer.AddToken(index::DictKeyInfo(key), 0);
                    keysInCurDoc.insert(ssKey.str());
                }
            }

            for (std::set<std::string>::const_iterator it = keysInCurDoc.begin(); it != keysInCurDoc.end(); ++it) {
                (*answer)[*it].push_back(i + baseDocId);
            }

            document::IndexDocument indexDocument(byteSlicePool);
            indexDocument.SetDocId(i);
            writer.EndDocument(indexDocument);
        }

        auto indexDir = segInfoDir->MakeDirectory(std::string(INDEX_DIR_NAME) + "/" + _indexName);
        assert(indexDir != nullptr);
        writer.Dump(indexDir, &simplePool, nullptr);
    }
    delete byteSlicePool;
}

void BitmapPostingMaker::MakeMultiSegmentsData(const std::vector<uint32_t>& docNums, Key2DocIdListMap* answer)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < docNums.size(); ++i) {
        segmentid_t segId = (segmentid_t)i;
        MakeOneSegmentData(segId, docNums[i], baseDocId, answer);
        baseDocId += docNums[i];
    }
}

} // namespace indexlib::index
