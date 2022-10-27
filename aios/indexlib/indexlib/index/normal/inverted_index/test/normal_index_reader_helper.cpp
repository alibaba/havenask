#include "indexlib/index/normal/inverted_index/test/normal_index_reader_helper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/util_define.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index/normal/inverted_index/accessor/index_format_writer_creator.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_writer.h"
#include "indexlib/util/path_util.h"

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);
using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, NormalIndexReaderHelper);

NormalIndexReaderHelper::NormalIndexReaderHelper() 
{
}

NormalIndexReaderHelper::~NormalIndexReaderHelper() 
{
}

void NormalIndexReaderHelper::PrepareSegmentFiles(
        string segmentPath, string indexName,
        IndexConfigPtr indexConfig, uint32_t docCount)
{
    storage::FileSystemWrapper::MkDirIfNotExist(segmentPath);
    DirectoryPtr segmentDir = DirectoryCreator::Create(segmentPath);
    DirectoryPtr singleIndexDir = segmentDir->MakeDirectory(
        PathUtil::JoinPath(INDEX_DIR_NAME, indexName));
    
    PrepareDictionaryFile(singleIndexDir, DICTIONARY_FILE_NAME, indexConfig);
    singleIndexDir->Store(POSTING_FILE_NAME, "", true);
    
    SegmentInfo segmentInfo;
    segmentInfo.docCount = docCount; 
    segmentInfo.Store(segmentDir);
}

void NormalIndexReaderHelper::PrepareDictionaryFile(
    const DirectoryPtr &directory, const string& dictFileName,
    const IndexConfigPtr& indexConfig)
{
    IndexFormatOptionPtr indexFormatOption(new IndexFormatOption());
    indexFormatOption->Init(indexConfig);
    util::SimplePool pool;
    IndexFormatWriterCreatorPtr writerCreator(new IndexFormatWriterCreator(
                    indexFormatOption, indexConfig));
    DictionaryWriterPtr dictionaryWriter(
            writerCreator->CreateDictionaryWriter(&pool));
    dictionaryWriter->Open(directory, dictFileName);
    dictionaryWriter->AddItem(1, 1);
    dictionaryWriter->Close();
}

IE_NAMESPACE_END(index);

