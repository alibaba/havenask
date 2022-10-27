#include "build_service/custom_merger/CustomMergeMeta.h"
#include <indexlib/storage/file_system_wrapper.h>

using namespace std;

IE_NAMESPACE_USE(storage);

namespace build_service {
namespace custom_merger {
BS_LOG_SETUP(custom_merger, CustomMergeMeta);

CustomMergeMeta::CustomMergeMeta() {
}

CustomMergeMeta::~CustomMergeMeta() {
}

void CustomMergeMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) 
{
    json.Jsonize("custom_merge_plans", _mergePlans, _mergePlans);
}

void CustomMergeMeta::store(const std::string& path)
{
    if (FileSystemWrapper::IsExist(path))
    {
        FileSystemWrapper::DeleteDir(path);
    }

    FileSystemWrapper::MkDir(path, true);
    string content = ToJsonString(*this);
    string mergeMetaFile = FileSystemWrapper::JoinPath(path, "merge_meta");
    FileSystemWrapper::AtomicStore(mergeMetaFile, content);
}

void CustomMergeMeta::load(const std::string& path)
{
    string mergeMetaFile = FileSystemWrapper::JoinPath(path, "merge_meta");
    string content;
    FileSystemWrapper::AtomicLoad(mergeMetaFile, content);
    FromJsonString(*this, content);
}

bool CustomMergeMeta::getMergePlan(size_t instanceId, CustomMergePlan& plan) const
{
    for (auto& mergePlan : _mergePlans) {
        if ((size_t)mergePlan.getInstanceId() == instanceId) {
            plan = mergePlan;
            return true;
        }
    }
    return false;
}

}
}
