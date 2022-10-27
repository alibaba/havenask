#include "indexlib/file_system/swap_mmap_file_reader.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SwapMmapFileReader);

SwapMmapFileReader::SwapMmapFileReader(const SwapMmapFileNodePtr& fileNode)
    : mFileNode(fileNode)
{
}

SwapMmapFileReader::~SwapMmapFileReader() 
{
}

IE_NAMESPACE_END(file_system);

