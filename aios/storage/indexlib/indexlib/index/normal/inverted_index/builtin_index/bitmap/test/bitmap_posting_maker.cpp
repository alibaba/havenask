#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/bitmap_posting_maker.h"

#include <sstream>

#include "autil/HashAlgorithm.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexWriter.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/util/Random.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::util;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, BitmapPostingMaker);

// TODO: add null
uint32_t BitmapPostingMaker::RandomTokenNum() { return dev_urandom() % 10 + 5; }

uint32_t BitmapPostingMaker::OneTokenNum() { return 1; }

BitmapPostingMaker::BitmapPostingMaker(const file_system::DirectoryPtr& rootDir, const string& indexName,
                                       bool enableNullTerm)
    : mRootDir(rootDir)
    , mIndexName(indexName)
    , mEnableNullTerm(enableNullTerm)
{
    srand(8888);
}

BitmapPostingMaker::~BitmapPostingMaker() {}

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
    std::string segInfoName = std::string(SEGMENT_FILE_NAME_PREFIX) + "_" + std::to_string(segId) + "_level_0/";
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    mRootDir->RemoveDirectory(segInfoName, removeOption /* may not exist */);
    auto segInfoDir = mRootDir->MakeDirectory(segInfoName);
    assert(segInfoDir != nullptr);

    // write segment info
    SegmentInfo segInfo;
    segInfo.docCount = docNum;
    segInfo.Store(segInfoDir);
    if (docNum == 0) {
        return;
    }

    Pool* byteSlicePool = new Pool(4 * 1024 * 1024);
    SimplePool simplePool;
    {
        BitmapIndexWriter writer(byteSlicePool, &simplePool, false);
        for (uint32_t i = 0; i < docNum; ++i) {
            uint32_t tokenNum = tokenNumFunc();
            if (mEnableNullTerm && (i % 4) == 1) {
                tokenNum = 0;
            }

            set<string> keysInCurDoc;
            if (tokenNum == 0) {
                // add null term token
                writer.AddToken(DictKeyInfo::NULL_TERM, 0);
                keysInCurDoc.insert("__NULL__");
            } else {
                for (uint32_t j = 0; j < tokenNum; ++j) {
                    stringstream ssKey;
                    ssKey << "token" << (j % 7);
                    uint64_t key = HashAlgorithm::hashString64(ssKey.str().c_str());

                    writer.AddToken(DictKeyInfo(key), 0);
                    keysInCurDoc.insert(ssKey.str());
                }
            }

            for (set<string>::const_iterator it = keysInCurDoc.begin(); it != keysInCurDoc.end(); ++it) {
                (*answer)[*it].push_back(i + baseDocId);
            }

            IndexDocument indexDocument(byteSlicePool);
            indexDocument.SetDocId(i);
            writer.EndDocument(indexDocument);
        }

        auto indexDir = segInfoDir->MakeDirectory(std::string(INDEX_DIR_NAME) + "/" + mIndexName);
        assert(indexDir != nullptr);
        writer.Dump(indexDir, &simplePool, nullptr);
    }
    delete byteSlicePool;
}

void BitmapPostingMaker::MakeMultiSegmentsData(const vector<uint32_t>& docNums, Key2DocIdListMap* answer)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < docNums.size(); ++i) {
        segmentid_t segId = (segmentid_t)i;
        MakeOneSegmentData(segId, docNums[i], baseDocId, answer);
        baseDocId += docNums[i];
    }
}
}}} // namespace indexlib::index::legacy
