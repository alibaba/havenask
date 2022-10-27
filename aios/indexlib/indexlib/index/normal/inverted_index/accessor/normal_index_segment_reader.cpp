#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/util/path_util.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/config/index_config.h"

using namespace std;
using future_lite::Future;
using future_lite::Promise;
using future_lite::Try;
using future_lite::Unit;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NormalIndexSegmentReader);

NormalIndexSegmentReader::NormalIndexSegmentReader() 
{
}

NormalIndexSegmentReader::~NormalIndexSegmentReader() 
{
}

void NormalIndexSegmentReader::Open(const config::IndexConfigPtr& indexConfig,
                                    const file_system::DirectoryPtr& indexDirectory)
{
    string dictFilePath = DICTIONARY_FILE_NAME;
    string postingFilePath = POSTING_FILE_NAME;
    string formatFilePath = INDEX_FORMAT_OPTION_FILE_NAME;
    bool dictExist = indexDirectory->IsExist(dictFilePath);
    bool postingExist = indexDirectory->IsExist(postingFilePath);
    if (!dictExist && !postingExist)
    {
        return;
    }

    if (!dictExist || !postingExist)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "index[%s]:missing dictionary or posting file", 
                             indexConfig->GetIndexName().c_str());
    }

    mDictReader.reset(CreateDictionaryReader(indexConfig));
    mDictReader->Open(indexDirectory, dictFilePath);

    mPostingReader = indexDirectory->CreateFileReader(postingFilePath,
            FSOT_LOAD_CONFIG);
    if (indexDirectory->IsExist(formatFilePath))
    {
        string indexFormatStr;
        indexDirectory->Load(formatFilePath, indexFormatStr);
        mOption = IndexFormatOption::FromString(indexFormatStr);
        mOption.SetNumberIndex(IndexFormatOption::IsNumberIndex(indexConfig));
    }
    else
    {
        mOption.Init(indexConfig);
        mOption.SetIsCompressedPostingHeader(false);
    }    
}

DictionaryReader* NormalIndexSegmentReader::CreateDictionaryReader(
        const IndexConfigPtr& indexConfig)
{
    return DictionaryCreator::CreateReader(indexConfig);
}

void NormalIndexSegmentReader::Open(const IndexConfigPtr& indexConfig,
                                    const index_base::SegmentData& segmentData)
{
    assert(indexConfig);
    mSegmentData = segmentData;
    file_system::DirectoryPtr indexDirectory = segmentData.GetIndexDirectory(
            indexConfig->GetIndexName(), false);
    if (!indexDirectory)
    {
        IE_LOG(INFO, "[%s] not exist in segment [%d]",
               indexConfig->GetIndexName().c_str(), segmentData.GetSegmentId());
        return;
    }
    Open(indexConfig, indexDirectory);
}

bool NormalIndexSegmentReader::GetSegmentPosting(dictkey_t key, 
        docid_t baseDocId, SegmentPosting &segPosting,
        autil::mem_pool::Pool* sessionPool) const
{
    dictvalue_t dictValue;
    if (!mDictReader || !mDictReader->Lookup(key, dictValue))
    {
        return false;
    }
    GetSegmentPosting(dictValue, segPosting);
    return true;
}

Future<bool> NormalIndexSegmentReader::GetSegmentPostingAsync(dictkey_t key, docid_t baseDocId,
    index::SegmentPosting& segPosting, autil::mem_pool::Pool* sessionPool) const
{
    if (!mDictReader)
    {
        return future_lite::makeReadyFuture<bool>(false);
    }
    file_system::ReadOption option;
    option.advice = storage::IO_ADVICE_LOW_LATENCY;    
    return mDictReader->LookupAsync(key, option)
        .thenValue([this, &segPosting](std::pair<bool, dictvalue_t> lookupResult) mutable {
            if (!lookupResult.first)
            {
                return future_lite::makeReadyFuture<bool>(false);
            }
            return GetSegmentPostingAsync(lookupResult.second, segPosting).thenValue([](Unit u) {
                return true;
            });
        });
}

void NormalIndexSegmentReader::GetSegmentPosting(dictvalue_t dictValue,
        SegmentPosting &segPosting) const
{
    PostingFormatOption formatOption = mOption.GetPostingFormatOption();
    segPosting.SetPostingFormatOption(formatOption);

    if (ShortListOptimizeUtil::IsDictInlineCompressMode(dictValue))
    {
        segPosting.Init(mSegmentData.GetBaseDocId(), 
                        mSegmentData.GetSegmentInfo(),
                        dictValue);
        return;
    }

    int64_t postingOffset = 0;
    ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);
    uint32_t postingLen;
    if (formatOption.IsCompressedPostingHeader())
    {
        postingLen = mPostingReader->ReadVUInt32(postingOffset);
        postingOffset += VByteCompressor::GetVInt32Length(postingLen);
    }
    else
    {
        mPostingReader->Read(&postingLen, sizeof(uint32_t), postingOffset);
        postingOffset += sizeof(uint32_t);
    }

    ByteSliceList* sliceList = mPostingReader->Read(postingLen, postingOffset);

    segPosting.Init(ByteSliceListPtr(sliceList), 
                    mSegmentData.GetBaseDocId(), mSegmentData.GetSegmentInfo(),
                    dictValue);
    return;
}

Future<future_lite::Unit> NormalIndexSegmentReader::GetSegmentPostingAsync(
    dictvalue_t dictValue, index::SegmentPosting& segPosting) const
{
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(dictValue))
    {
        return future_lite::makeReadyFuture([this, dictValue, &segPosting]() {
            PostingFormatOption formatOption = mOption.GetPostingFormatOption();
            segPosting.SetPostingFormatOption(formatOption);
            segPosting.Init(mSegmentData.GetBaseDocId(), mSegmentData.GetSegmentInfo(), dictValue);
            return Unit();
        }());
    }

    int64_t postingOffset = 0;
    ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);

    ReadOption option;
    option.advice = storage::IO_ADVICE_LOW_LATENCY;

    auto initSliceList = [this, dictValue, &segPosting](
                             uint32_t postingLen, int64_t postingOffset, ReadOption option) {
        auto sliceList = mPostingReader->Read(postingLen, postingOffset);
        segPosting.SetPostingFormatOption(mOption.GetPostingFormatOption());
        // NOTICE: SegmentPosting Init has IO inside, but in most case, this area is already in block cache
        //  since previous postingLen readed
        segPosting.Init(ByteSliceListPtr(sliceList), mSegmentData.GetBaseDocId(),
                        mSegmentData.GetSegmentInfo(), dictValue);
    };

    if (mOption.GetPostingFormatOption().IsCompressedPostingHeader())
    {
        return mPostingReader->ReadVUInt32Async(postingOffset, option)
            .thenValue([this, postingOffset, option, postWork = std::move(initSliceList)](size_t postingLen) mutable {
                postingOffset += VByteCompressor::GetVInt32Length(postingLen);
                return postWork(postingLen, postingOffset, option);
            });
    }
    else
    {
        return mPostingReader->ReadUInt32Async(postingOffset, option)
            .thenValue([this, postingOffset, option, postWork = std::move(initSliceList)](
                           uint32_t postingLen) mutable {
                postingOffset += sizeof(uint32_t);
                return postWork(postingLen, postingOffset, option);
            });
    }
}

IE_NAMESPACE_END(index);

