#include "build_service/config/HashMode.h"

using namespace std;
namespace build_service {
namespace config {

BS_LOG_SETUP(config, HashMode);
BS_LOG_SETUP(config, RegionHashMode);

HashMode::HashMode() {
    _hashFunction = "HASH";
    _hashField = "";
}

HashMode::~HashMode() {
}

void HashMode::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("hash_function", _hashFunction, _hashFunction);
    json.Jsonize("hash_field", _hashField, _hashField);
    json.Jsonize("hash_fields", _hashFields, _hashFields);
    json.Jsonize("hash_params", _hashParams, _hashParams);
    if (!_hashField.empty() && _hashFields.empty()) {
        _hashFields.push_back(_hashField);
    }
}

bool HashMode::validate() const {
    if (!_hashField.empty() && _hashFields.empty()) {
        _hashFields.push_back(_hashField); // compatible with direct set hash filed
    }

    if (_hashFields.size() == 0 || _hashFields[0].empty()) {
        BS_LOG(ERROR, "hash field is empty. ");
        return false;
    }
    if (_hashFunction.empty()) {
        BS_LOG(WARN, "hash function is empty. ");
        return false;
    }

    return true;
}

const string RegionHashMode::REGION_NAME = "region_name";

RegionHashMode::RegionHashMode() {
    _regionName = "";
}

RegionHashMode::~RegionHashMode() {
}

void RegionHashMode::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    HashMode::Jsonize(json);
    json.Jsonize(RegionHashMode::REGION_NAME, _regionName, _regionName);
}

bool RegionHashMode::validate() const {
    if (!HashMode::validate()) {
        return false;
    }
    if (_regionName.empty()) {
        BS_LOG(ERROR, "region name is empty. ");
        return false;

    }
    return true;
}

}
}
