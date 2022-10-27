#include <sstream>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <autil/HashAlgorithm.h>
#include "indexlib/misc/random.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/bitmap_posting_maker.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/file_system/directory_creator.h"
#include <autil/mem_pool/SimpleAllocator.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BitmapPostingMaker);

uint32_t BitmapPostingMaker::RandomTokenNum()
{
    return misc::dev_urandom() % 10 + 5;
}

uint32_t BitmapPostingMaker::OneTokenNum()
{
    return 1;
}

BitmapPostingMaker::BitmapPostingMaker(const string& rootDir,
                                       const string& indexName) 
    : mRootDir(rootDir)
    , mIndexName(indexName)
{
    srand(8888);
}

BitmapPostingMaker::~BitmapPostingMaker() 
{
}

void BitmapPostingMaker::MakeOnePostingList(segmentid_t segId, 
        uint32_t docNum, docid_t baseDocId, PostingList& answer)
{
    Key2DocIdListMap key2DocIdListMap;
    InnerMakeOneSegmentData(segId, docNum, baseDocId, 
                            key2DocIdListMap, OneTokenNum);

    answer.first = baseDocId;
    answer.second = key2DocIdListMap.begin()->second;
}

void BitmapPostingMaker::MakeOneSegmentData(segmentid_t segId, uint32_t docNum,
                            docid_t baseDocId, Key2DocIdListMap& answer)
{
    InnerMakeOneSegmentData(segId, docNum, baseDocId, 
                            answer, RandomTokenNum);
}

void BitmapPostingMaker::InnerMakeOneSegmentData(segmentid_t segId,
        uint32_t docNum, docid_t baseDocId, 
        Key2DocIdListMap& answer, TokenNumFunc tokenNumFunc)
{
    stringstream ss;
    ss << mRootDir << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_0/";
    IndexTestUtil::ResetDir(ss.str()); 

    // write segment info
    SegmentInfo segInfo;
    segInfo.docCount = docNum;
    string segInfoFileName = ss.str() + SEGMENT_INFO_FILE_NAME;
    segInfo.Store(segInfoFileName);

    ss << INDEX_DIR_NAME << "/";
    IndexTestUtil::ResetDir(ss.str());
    ss << mIndexName << "/";
    IndexTestUtil::ResetDir(ss.str());
    
    if (docNum == 0)
    {
        return;
    }
    
    Pool *byteSlicePool = new Pool(4 * 1024 * 1024);
    SimplePool simplePool;
    {        
        BitmapIndexWriter writer(byteSlicePool, &simplePool, false);
        for (uint32_t i = 0; i < docNum; ++i)
        {
            uint32_t tokenNum = tokenNumFunc();

            set<string> keysInCurDoc;
            for (uint32_t j = 0; j < tokenNum; ++j)
            {
                stringstream ssKey;
                ssKey << "token" << (j % 7);
                uint64_t key = HashAlgorithm::hashString64(ssKey.str().c_str());

                writer.AddToken(key, 0);
                keysInCurDoc.insert(ssKey.str());
            }

            for (set<string>::const_iterator it = keysInCurDoc.begin();
                 it != keysInCurDoc.end(); ++it)
            {
                answer[*it].push_back(i + baseDocId);
            }

            IndexDocument indexDocument(byteSlicePool);
            indexDocument.SetDocId(i);
            writer.EndDocument(indexDocument);
        }

        writer.Dump(DirectoryCreator::Create(ss.str()), &simplePool);
    }
    delete byteSlicePool;
}

void BitmapPostingMaker::MakeMultiSegmentsData(const vector<uint32_t>& docNums,
        Key2DocIdListMap& answer)
{
    docid_t baseDocId = 0;
    for (size_t i = 0; i < docNums.size(); ++i)
    {
        segmentid_t segId = (segmentid_t)i;
        MakeOneSegmentData(segId, docNums[i], baseDocId, answer);
        baseDocId += docNums[i];
    }
}

IE_NAMESPACE_END(index);

