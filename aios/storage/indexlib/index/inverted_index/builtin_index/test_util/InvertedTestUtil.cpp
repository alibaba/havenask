#include "indexlib/index/inverted_index/builtin_index/test_util/InvertedTestUtil.h"

#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapIndexReader.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"

namespace indexlib::index {

namespace {
using indexlibv2::framework::MetricsManager;
using indexlibv2::framework::Segment;
using indexlibv2::framework::SegmentInfo;
using indexlibv2::framework::Version;
using indexlibv2::index::IndexerParameter;
} // namespace

std::shared_ptr<InvertedIndexReaderImpl>
InvertedTestUtil::CreateIndexReader(const std::vector<uint32_t>& docNums,
                                    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
{
    auto indexReader = std::make_shared<InvertedIndexReaderImpl>(/*InvertedIndexMetrics=*/nullptr);
    Version version;
    for (size_t i = 0; i < docNums.size(); i++) {
        version.AddSegment(i);
    }
    Indexers indexers;
    auto status = GetIndexers(_dir, indexConfig, version, indexers);
    if (!status.IsOK()) {
        return nullptr;
    }
    status = indexReader->DoOpen(indexConfig, indexers);
    if (!status.IsOK()) {
        return nullptr;
    }
    return indexReader;
}

std::shared_ptr<InvertedMemIndexer>
InvertedTestUtil::CreateIndexWriter(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                    bool resetHighFreqVol)
{
    std::shared_ptr<framework::SegmentMetrics> metrics(new framework::SegmentMetrics);
    metrics->SetDistinctTermCount(indexConfig->GetIndexName(), HASHMAP_INIT_SIZE);
    IndexerParameter param;
    auto writer = std::make_shared<InvertedMemIndexer>(param, nullptr);
    auto extractorFactory = std::make_shared<indexlibv2::plain::DocumentInfoExtractorFactory>();
    auto s = writer->Init(indexConfig, extractorFactory.get());
    if (!s.IsOK()) {
        return nullptr;
    }
    if (resetHighFreqVol) {
        writer->_highFreqVol.reset();
    }
    return writer;
}

bool InvertedTestUtil::SetUp()
{
    file_system::FslibWrapper::DeleteDirE(_dirStr, file_system::DeleteOption::NoFence(true));
    file_system::FslibWrapper::MkDirE(_dirStr);

    file_system::FileSystemOptions fileSystemOptions;
    fileSystemOptions.needFlush = true;
    std::shared_ptr<util::MemoryQuotaController> mqc(
        new util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
    std::shared_ptr<util::PartitionMemoryQuotaController> quotaController(
        new util::PartitionMemoryQuotaController(mqc));
    fileSystemOptions.memoryQuotaController = quotaController;
    _fileSystem = file_system::FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                                         /*rootPath=*/_dirStr, fileSystemOptions,
                                                         std::shared_ptr<util::MetricProvider>(),
                                                         /*isOverride=*/false)
                      .GetOrThrow();

    std::shared_ptr<file_system::Directory> directory =
        file_system::IDirectory::ToLegacyDirectory(std::make_shared<file_system::MemDirectory>("", _fileSystem));
    _dir = directory;
    _fileSystem->Sync(true).GetOrThrow();

    _byteSlicePool = new autil::mem_pool::Pool(&_allocator, DEFAULT_CHUNK_SIZE);
    return true;
}

bool InvertedTestUtil::TearDown()
{
    if (file_system::FslibWrapper::IsExist(_dirStr).GetOrThrow()) {
        file_system::FslibWrapper::DeleteDirE(_dirStr, file_system::DeleteOption::NoFence(false));
    }
    // section meta may not be exist, but rm -rf can ignore error...
    std::string sectionPath = _dirStr + "_section";
    if (file_system::FslibWrapper::IsExist(sectionPath).GetOrThrow()) {
        file_system::FslibWrapper::DeleteDirE(sectionPath, file_system::DeleteOption::NoFence(false));
    }
    delete _byteSlicePool;
    return true;
}

Status InvertedTestUtil::GetIndexers(const std::shared_ptr<file_system::Directory>& dir,
                                     const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                     const Version& version, Indexers& indexers)
{
    docid_t baseDocId = 0;
    for (auto iter = version.begin(); iter != version.end(); ++iter) {
        auto segId = iter->segmentId;
        std::string segDirName = version.GetSegmentDirName(segId);
        // std::string segInfoFile = segDirName + "/segment_info";
        // if (!dir->IsExist(segInfoFile)) {
        //     return Status::InternalError("seg info file not exist");
        // }
        // std::string segInfoStr;
        // dir->Load(segInfoFile, segInfoStr);
        // if (segInfoStr.empty()) {
        //     return Status::InternalError("seg info str empty");
        // }
        auto segInfo = std::make_shared<SegmentInfo>();
        // segInfo->FromString(segInfoStr);
        auto segDir = dir->GetIDirectory()->GetDirectory(segDirName).GetOrThrow();
        auto status = segInfo->Load(segDir, file_system::ReaderOption(file_system::FSOT_MEM));
        if (!status.IsOK()) {
            return status;
        }
        auto docCount = segInfo->docCount;
        if (docCount == 0) {
            continue;
        }
        auto segStatus = Segment::SegmentStatus::ST_BUILT;
        std::shared_ptr<kmonitor::MetricsReporter> metricsReporter;
        std::shared_ptr<MetricsManager> metricsManager(new MetricsManager("", metricsReporter));
        IndexerParameter indexerParam;
        indexerParam.metricsManager = metricsManager.get();
        indexerParam.docCount = docCount;
        indexerParam.segmentInfo = segInfo;
        auto indexer = std::make_shared<InvertedDiskIndexer>(indexerParam);
        auto indexDir = dir->GetDirectory(segDirName + "/" + INDEX_DIR_NAME, false);
        if (indexDir == nullptr) {
            return Status::InternalError("index dir is nullptr");
        }
        status = indexer->Open(indexConfig, indexDir->GetIDirectory());
        if (!status.IsOK()) {
            return status;
        }
        indexers.emplace_back(baseDocId, indexer, segId, segStatus);
        baseDocId += docCount;
    }
    return Status::OK();
}

void InvertedTestUtil::DumpSegment(segmentid_t segmentId, const std::shared_ptr<InvertedMemIndexer>& writer,
                                   const SegmentInfo& segmentInfo)
{
    std::stringstream ss;
    ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0";
    std::shared_ptr<file_system::Directory> segDirectory = _dir->MakeDirectory(ss.str());
    std::shared_ptr<file_system::Directory> indexDirectory = segDirectory->MakeDirectory(INDEX_DIR_NAME);

    auto s = writer->Dump(&_simplePool, indexDirectory, nullptr);
    assert(s.IsOK());

    s = segmentInfo.Store(segDirectory->GetIDirectory());
    assert(s.IsOK());
}

} // namespace indexlib::index
