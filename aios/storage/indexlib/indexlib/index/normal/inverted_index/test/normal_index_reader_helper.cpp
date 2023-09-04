#include "indexlib/index/normal/inverted_index/test/normal_index_reader_helper.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/inverted_index/format/dictionary/UtilDefine.h"
#include "indexlib/index/normal/inverted_index/accessor/index_format_writer_creator.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/util/PathUtil.h"

using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index_base;

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, NormalIndexReaderHelper);

NormalIndexReaderHelper::NormalIndexReaderHelper() {}

NormalIndexReaderHelper::~NormalIndexReaderHelper() {}

void NormalIndexReaderHelper::PrepareSegmentFiles(const DirectoryPtr& segmentDir, string indexName,
                                                  std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig,
                                                  uint32_t docCount)
{
    DirectoryPtr singleIndexDir = segmentDir->MakeDirectory(PathUtil::JoinPath(INDEX_DIR_NAME, indexName));

    PrepareDictionaryFile(singleIndexDir, DICTIONARY_FILE_NAME, indexConfig);
    singleIndexDir->Store(POSTING_FILE_NAME, "", WriterOption::AtomicDump());

    SegmentInfo segmentInfo;
    segmentInfo.docCount = docCount;
    segmentInfo.Store(segmentDir);
}

void NormalIndexReaderHelper::PrepareDictionaryFile(
    const DirectoryPtr& directory, const string& dictFileName,
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
{
    util::SimplePool pool;
    std::shared_ptr<DictionaryWriter> dictionaryWriter(
        legacy::IndexFormatWriterCreator::CreateDictionaryWriter(indexConfig, "", &pool));
    dictionaryWriter->Open(directory, dictFileName);
    dictionaryWriter->AddItem(index::DictKeyInfo(1), 1);
    dictionaryWriter->Close();
}
}} // namespace indexlib::index
