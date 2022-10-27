#include "indexlib/file_system/indexlib_file_system_metrics.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, IndexlibFileSystemMetrics);

IndexlibFileSystemMetrics::IndexlibFileSystemMetrics()
{
}

IndexlibFileSystemMetrics::IndexlibFileSystemMetrics(
        const StorageMetrics& inMemStorageMetrics,
        const StorageMetrics& localStorageMetrics)
    : mInMemStorageMetrics(inMemStorageMetrics)
    , mLocalStorageMetrics(localStorageMetrics)
{
}

IndexlibFileSystemMetrics::~IndexlibFileSystemMetrics() 
{
}

IE_NAMESPACE_END(file_system);

