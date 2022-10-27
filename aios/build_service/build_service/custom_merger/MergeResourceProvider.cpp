#include "build_service/custom_merger/MergeResourceProvider.h"
#include "indexlib/partition/remote_access/partition_resource_provider_factory.h"
#include <indexlib/index_base/version_loader.h>

using namespace std;
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

namespace build_service {
namespace custom_merger {
BS_LOG_SETUP(custom_merger, MergeResourceProvider);

MergeResourceProvider::MergeResourceProvider()
    : _checkpointVersionId(-1)
{
}

MergeResourceProvider::~MergeResourceProvider() {
}

bool MergeResourceProvider::init(const std::string& indexPath,
                                 const IndexPartitionOptions& options,
                                 const IndexPartitionSchemaPtr& newSchema,
                                 const std::string& pluginPath,
                                 int32_t targetVersionId,
                                 int32_t checkpointVersionId)
{
    assert(newSchema);
    _newSchema = newSchema;
    _indexProvider = PartitionResourceProviderFactory::GetInstance()->CreateProvider(indexPath, options, pluginPath, targetVersionId);
    _checkpointVersionId = checkpointVersionId;
    return _indexProvider.get() != nullptr;
}

void MergeResourceProvider::getIndexSegmentInfos(
        std::vector<SegmentInfo>& segmentInfos)
{
    auto ite = _indexProvider->CreatePartitionSegmentIterator();
    Version checkpointVersion;
    if (_checkpointVersionId != -1) {
        VersionLoader::GetVersion(_indexProvider->GetIndexPath(), checkpointVersion, _checkpointVersionId);
    }

    while (ite->IsValid()) {
        if (checkpointVersion.HasSegment(ite->GetSegmentId())) {
            ite->MoveToNext();
            continue;
        }
        SegmentInfo info;
        info.segmentId = ite->GetSegmentId();
        info.docCount = ite->GetSegmentData().GetSegmentInfo().docCount;
        segmentInfos.push_back(info);
        ite->MoveToNext();
    }
}

}
}
