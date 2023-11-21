#pragma once

#include "autil/mem_pool/Pool.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/kv/FieldValueExtractor.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/KVIndexFactory.h"
#include "indexlib/index/kv/MultiSegmentKVIterator.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class MultiSegmentKVIteratorTestBase : public TESTBASE
{
public:
    void setUp() override;

protected:
    void PrepareSegments(const std::vector<std::string>& segmentDocsVec,
                         std::vector<std::shared_ptr<framework::Segment>>& segments);

    void BuildIndex(const std::vector<std::string>& segmentDocsVec) const;

    void BuildOneSegment(const std::string& docsStr, size_t segmentId) const;

    std::shared_ptr<indexlib::file_system::Directory> MakeSegmentDirectory(size_t id) const;

    std::shared_ptr<framework::Segment> LoadSegment(const std::shared_ptr<indexlib::file_system::Directory>& dir,
                                                    size_t id, bool addIndex = false) const;

    void CheckIterator(MultiSegmentKVIterator* iter, const std::vector<std::pair<keytype_t, bool>>& keys,
                       const std::vector<std::vector<std::string>>& values) const;

protected:
    std::shared_ptr<config::TabletSchema> _schema;
    std::shared_ptr<config::KVIndexConfig> _config;
    MemIndexerParameter _indexerParam;
    std::string _rootDir;
    std::shared_ptr<indexlib::file_system::Directory> _indexRootDir;
    std::shared_ptr<indexlib::file_system::Directory> _onlineDirectory;
    std::unique_ptr<autil::mem_pool::Pool> _pool;
    std::unique_ptr<PackAttributeFormatter> _formatter;
};

} // namespace indexlibv2::index
