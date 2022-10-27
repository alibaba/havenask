#pragma once
#include <ha3/sql/common/common.h>
#include <build_service/config/HashMode.h>
BEGIN_HA3_NAMESPACE(sql);

typedef build_service::config::HashMode HashMode;

class TableDistribution : public autil::legacy::Jsonizable {
public:
    TableDistribution() {
        partCnt = 0;
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override {
        json.Jsonize("hash_mode", hashMode, hashMode);
        json.Jsonize("partition_cnt", partCnt, partCnt);
        json.Jsonize("hash_values", hashValues, hashValues);
        json.Jsonize("hash_values_sep", hashValuesSep, hashValuesSep);
        json.Jsonize("assigned_hash_values", debugHashValues, debugHashValues);
        json.Jsonize("assigned_partition_ids", debugPartIds, debugPartIds);
    }
    bool validate() {
        if (!hashMode.validate()) {
            return false;
        }
    }
public:
    std::string debugHashValues;
    std::string debugPartIds;
    HashMode hashMode;
    int32_t partCnt;
    std::map<std::string, std::vector<std::string> > hashValues;
    std::map<std::string, std::string> hashValuesSep;
};

END_HA3_NAMESPACE(sql);
