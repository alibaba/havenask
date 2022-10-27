#pragma once
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <unittest/unittest.h>
#include <khronos/indexlib_plugin/test/KhronosTestUtil.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/config/index_partition_options.h>
#include <indexlib/partition/index_partition_resource.h>
#include <indexlib/partition/index_application.h>
#include <indexlib/test/partition_state_machine.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosTestTableBuilder
{
public:
public:
    KhronosTestTableBuilder();
    ~KhronosTestTableBuilder();
    KhronosTestTableBuilder(const KhronosTestTableBuilder &) = delete;
    KhronosTestTableBuilder& operator=(const KhronosTestTableBuilder &) = delete;
public:
    auto *getIndexApp() {
        assert(_buildSuccessFlag);
        return _indexApp.get();
    }
public:
    bool build(const std::string &dataRootDir);
    bool build(const std::string &dataRootDir,
               const khronos::indexlib_plugin::KhronosTestUtil::BinaryTsDataList &binaryTsDataList);
private:
    bool getBinaryTsData(khronos::indexlib_plugin::KhronosTestUtil::BinaryTsDataList &out);
private:
    bool _buildCallFlag;
    bool _buildSuccessFlag;
    // do not change order !!!
    autil::mem_pool::Pool _pool;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;
    IE_NAMESPACE(config)::IndexPartitionOptions _options;
    IE_NAMESPACE(partition)::IndexPartitionResource _partitionResource;
    IE_NAMESPACE(test)::PartitionStateMachinePtr _psm;
    IE_NAMESPACE(partition)::IndexApplicationPtr _indexApp;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KhronosTestTableBuilder);
END_HA3_NAMESPACE(sql);
