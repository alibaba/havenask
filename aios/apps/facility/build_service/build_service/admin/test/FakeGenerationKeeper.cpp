#include "build_service/admin/test/FakeGenerationKeeper.h"

#include "autil/Thread.h"
#include "build_service/admin/GenerationTask.h"
#include "build_service/admin/catalog/CatalogVersionPublisher.h"
#include "build_service/common/PathDefine.h"
#include "build_service/proto/ProtoComparator.h"

using namespace std;

using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::util;

namespace build_service { namespace admin {

bool gDelegateFake = false;
std::string gDelegateIndexRoot = "";
vector<string> gDelegateClusters;
set<proto::BuildId> gRecoverFailBuildId;
proto::BuildId myBuildId;
OperationQueue gOperationQueue;
autil::ThreadMutex setLock;

void setRecoverFail(const proto::BuildId& buildId)
{
    autil::ScopedLock lock(setLock);
    gRecoverFailBuildId.insert(buildId);
}

void setRecoverSucc(const proto::BuildId& buildId)
{
    autil::ScopedLock lock(setLock);
    gRecoverFailBuildId.erase(buildId);
}

void setDelegateToFake(bool delegate) { gDelegateFake = delegate; }

OperationQueue& getOperationQueue() { return gOperationQueue; }

GenerationKeeper::GenerationKeeper(const Param& param)
    : _buildId(param.buildId)
    , _generationTask(NULL)
    , _zkRoot(param.zkRoot)
    , _adminServiceName(param.adminServiceName)
    , _amonitorPort(param.amonitorPort)
    , _generationDir(PathDefine::getGenerationZkRoot(param.zkRoot, param.buildId))
    , _zkWrapper(param.zkWrapper)
    , _zkStatus(param.zkWrapper,
                fslib::util::FileUtil::joinFilePath(_generationDir, PathDefine::ZK_GENERATION_STATUS_FILE))
    , _isStopped(false)
    , _needSyncStatus(false)
    , _workerTable(new WorkerTable(param.zkRoot, param.zkWrapper))
    , _prohibitIpCollector(param.prohibitIpCollector)
    , _startingBuildTimestamp(0)
    , _stoppingBuildTimestamp(0)
    , _refreshTimestamp(0)
    , _reporter(param.monitor, false)
    , _catalogRpcChannelManager(param.catalogRpcChannelManager)
    , _catalogPartitionIdentifier(param.catalogPartitionIdentifier)
    , _specifiedWorkerPkgList(param.specifiedWorkerPkgList)
{
    if (_catalogPartitionIdentifier && _catalogRpcChannelManager) {
        _catalogVersionPublisher = std::make_unique<CatalogVersionPublisher>(
            _catalogPartitionIdentifier, _buildId.generationid(), _catalogRpcChannelManager);
    }
}

bool GenerationKeeper::recoverStoppedKeeper()
{
    if (gDelegateFake) {
        _isStopped = true;
        autil::ScopedLock lock(setLock);
        if (gRecoverFailBuildId.find(_buildId) != gRecoverFailBuildId.end()) {
            return false;
        }
        return true;
    } else {
        typedef bool (*func_type)(GenerationKeeper*);
        func_type func = GET_NEXT_FUNC(func_type);
        return func(this);
    }
}

bool GenerationKeeper::recover()
{
    if (gDelegateFake) {
        autil::ScopedLock lock(setLock);
        if (gRecoverFailBuildId.find(_buildId) != gRecoverFailBuildId.end()) {
            return false;
        }
        return true;
    } else {
        typedef bool (*func_type)(GenerationKeeper*);
        func_type func = GET_NEXT_FUNC(func_type);
        return func(this);
    }
}

bool GenerationKeeper::updateConfig(const string& configPath)
{
    if (gDelegateFake) {
        gOperationQueue.push_back(make_pair(myBuildId, "__upc__" + configPath));
        return true;
    } else {
        typedef bool (*func_type)(GenerationKeeper*, const string&);
        func_type func = GET_NEXT_FUNC(func_type);
        return func(this, configPath);
    }
}

// bool GenerationKeeper::isIndexDeleted() const {
//     if (gDelegateFake) {
//         return true;
//     } else {
//         typedef bool (*func_type)(const GenerationKeeper *);
//         func_type func = GET_NEXT_FUNC(func_type);
//         return func(this);
//     }
// }

bool GenerationKeeper::createVersion(const std::string& clusterName, const std::string& mergeTask)
{
    if (gDelegateFake) {
        gOperationQueue.push_back(make_pair(myBuildId, string("__createversion__" + clusterName)));
        return true;
    } else {
        typedef bool (*func_type)(GenerationKeeper*, const std::string&, const std::string&);
        func_type func = GET_NEXT_FUNC(func_type);
        assert(func);
        return func(this, clusterName, mergeTask);
    }
}

void GenerationKeeper::stepBuild(proto::InformResponse* response)
{
    if (gDelegateFake) {
        return;
    } else {
        typedef void (*func_type)(GenerationKeeper*, proto::InformResponse*);
        func_type func = GET_NEXT_FUNC(func_type);
        func(this, response);
    }
}

bool GenerationKeeper::stopBuild()
{
    if (gDelegateFake) {
        return true;
    } else {
        typedef bool (*func_type)(const GenerationKeeper*);
        func_type func = GET_NEXT_FUNC(func_type);
        return func(this);
    }
}

bool GenerationKeeper::switchBuild()
{
    if (gDelegateFake) {
        return true;
    } else {
        typedef bool (*func_type)(const GenerationKeeper*);
        func_type func = GET_NEXT_FUNC(func_type);
        return func(this);
    }
    return true;
}

bool GenerationKeeper::getTotalRemainIndexSize(int64_t& totalSize)
{
    if (gDelegateFake) {
        return true;
    } else {
        typedef bool (*func_type)(const GenerationKeeper*, int64_t&);
        func_type func = GET_NEXT_FUNC(func_type);
        return func(this, totalSize);
    }
    return true;
}

const string& GenerationKeeper::getIndexRoot() const
{
    if (gDelegateFake) {
        return gDelegateIndexRoot;
    } else {
        typedef const string& (*func_type)(const GenerationKeeper*);
        func_type func = GET_NEXT_FUNC(func_type);
        return func(this);
    }
    return gDelegateIndexRoot;
}

vector<string> GenerationKeeper::getClusterNames() const
{
    if (gDelegateFake) {
        return gDelegateClusters;
    } else {
        typedef vector<string> (*func_type)(const GenerationKeeper*);
        func_type func = GET_NEXT_FUNC(func_type);
        return func(this);
    }
    return gDelegateClusters;
}

}} // namespace build_service::admin
