#pragma once

#include <memory>
#include <tuple>
#include <vector>

#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::file_system {
class Directory;
class IFileSystem;
} // namespace indexlib::file_system

namespace indexlib::config {
class IndexPartitionSchema;
class HighFrequencyVocabulary;
} // namespace indexlib::config

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlibv2::framework {
class Version;
}
namespace indexlibv2::index {
class IIndexer;
struct DiskIndexerParameter;
} // namespace indexlibv2::index
namespace indexlib::index {

class InvertedDiskIndexer;
class InvertedMemIndexer;

class InvertedTestUtil
{
public:
    explicit InvertedTestUtil(const std::string& dirStr, const std::shared_ptr<config::HighFrequencyVocabulary>& vol)
        : _dirStr(dirStr)
        , _vol(vol)
    {
    }
    using Indexers = std::vector<InvertedIndexReaderImpl::Indexer>;

    bool SetUp();
    bool TearDown();

    std::shared_ptr<InvertedIndexReaderImpl>
    CreateIndexReader(const std::vector<uint32_t>& docNums,
                      const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);

    std::shared_ptr<InvertedMemIndexer>
    CreateIndexWriter(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                      bool resetHighFreqVol);

    void DumpSegment(segmentid_t segmentId, const std::shared_ptr<InvertedMemIndexer>& writer,
                     const indexlibv2::framework::SegmentInfo& segmentInfo);

    Status GetIndexers(const std::shared_ptr<file_system::Directory>& dir,
                       const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                       const indexlibv2::framework::Version& version, Indexers& indexers);

private:
    std::string _dirStr;
    std::shared_ptr<file_system::Directory> _dir;
    std::shared_ptr<file_system::IFileSystem> _fileSystem;
    std::shared_ptr<config::IndexPartitionSchema> _schema;

    autil::mem_pool::SimpleAllocator _allocator;
    autil::mem_pool::Pool* _byteSlicePool;
    util::SimplePool _simplePool;
    std::shared_ptr<config::HighFrequencyVocabulary> _vol;
};

} // namespace indexlib::index
