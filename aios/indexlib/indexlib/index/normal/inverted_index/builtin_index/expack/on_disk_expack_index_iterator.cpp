#include <fslib/fs/FileSystem.h>
#include "indexlib/index/normal/inverted_index/builtin_index/expack/on_disk_expack_index_iterator.h"

using namespace std;


IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, OnDiskExpackIndexIterator);

OnDiskExpackIndexIterator::OnDiskExpackIndexIterator(
        const config::IndexConfigPtr& indexConfig,        
        const DirectoryPtr& indexDirectory,
        const PostingFormatOption& postingFormatOption,
        const config::MergeIOConfig &ioConfig)
    : OnDiskPackIndexIterator(indexConfig, indexDirectory,
                              postingFormatOption, ioConfig)
{
}

OnDiskExpackIndexIterator::~OnDiskExpackIndexIterator() 
{
}

IE_NAMESPACE_END(index);

