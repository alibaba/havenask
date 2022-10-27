#ifndef __INDEXLIB_NORMAL_INDEX_READER_HELPER_H
#define __INDEXLIB_NORMAL_INDEX_READER_HELPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index);

class NormalIndexReaderHelper
{
public:
    NormalIndexReaderHelper();
    ~NormalIndexReaderHelper();
public:
    static void PrepareSegmentFiles(std::string segmentPath,
                                    std::string indexPath,
                                    config::IndexConfigPtr indexConfig, uint32_t docCount = 1);

    static void PrepareDictionaryFile(const file_system::DirectoryPtr &directory,
                                      const std::string& dictFileName,
                                      const config::IndexConfigPtr& indexConfig);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalIndexReaderHelper);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_NORMAL_INDEX_READER_HELPER_H
