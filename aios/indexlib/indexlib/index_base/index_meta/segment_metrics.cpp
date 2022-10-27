#include <autil/legacy/json.h>
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentMetrics);

const string SegmentMetrics::BUILT_IN_TERM_COUNT_METRICS = "__@__term_count";

SegmentMetrics::SegmentMetrics() 
{
}

SegmentMetrics::~SegmentMetrics() 
{
}

void SegmentMetrics::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("groups", mGroupMap);
}

size_t SegmentMetrics::GetDistinctTermCount(const std::string &indexName) const
{
    size_t ret = 0;
    Get<size_t>(BUILT_IN_TERM_COUNT_METRICS, indexName, ret);
    return ret;
}

void SegmentMetrics::SetDistinctTermCount(const std::string &indexName, size_t distinctTermCount)
{
    Set<size_t>(BUILT_IN_TERM_COUNT_METRICS, indexName, distinctTermCount);
}

string SegmentMetrics::ToString() const
{
    return json::ToString(mGroupMap);
}

void SegmentMetrics::FromString(const string& str)
{
    Any a = json::ParseJson(str);
    FromJson(mGroupMap, a);
}

void SegmentMetrics::Store(const DirectoryPtr& directory) const
{
    const string& content = ToString();
    if (directory->IsExist(SEGMETN_METRICS_FILE_NAME))
    {
        directory->RemoveFile(SEGMETN_METRICS_FILE_NAME);
    }
    directory->Store(SEGMETN_METRICS_FILE_NAME, content, false);
}

void SegmentMetrics::Load(const DirectoryPtr& directory)
{
    string content;
    directory->Load(SEGMETN_METRICS_FILE_NAME, content, true);
    FromString(content);
}

void SegmentMetrics::Store(const string& path) const
{
    const string& content = ToString();
    string filePath = FileSystemWrapper::JoinPath(
            path, SEGMETN_METRICS_FILE_NAME);
    FileSystemWrapper::AtomicStoreIgnoreExist(filePath, content);
}

void SegmentMetrics::Load(const string& path)
{
    string filePath = FileSystemWrapper::JoinPath(
            path, SEGMETN_METRICS_FILE_NAME);
    string content;
    FileSystemWrapper::AtomicLoad(filePath, content);
    FromString(content);
}

IE_NAMESPACE_END(index_base);

