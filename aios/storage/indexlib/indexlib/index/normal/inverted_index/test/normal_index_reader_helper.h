#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index {

class NormalIndexReaderHelper
{
public:
    NormalIndexReaderHelper();
    ~NormalIndexReaderHelper();

public:
    static void PrepareSegmentFiles(const file_system::DirectoryPtr& segmentDir, std::string indexPath,
                                    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> indexConfig,
                                    uint32_t docCount = 1);

    static void PrepareDictionaryFile(const file_system::DirectoryPtr& directory, const std::string& dictFileName,
                                      const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalIndexReaderHelper);
}} // namespace indexlib::index
