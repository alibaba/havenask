#include <ha3/common/RankSortDescription.h>
#include <autil/StringUtil.h>

using namespace autil;
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, RankSortDescription);

RankSortDescription::RankSortDescription() {
    _percent = 100.0;
}

RankSortDescription::~RankSortDescription() {
    for (size_t i = 0; i < _sortDescs.size(); ++i) {
        delete _sortDescs[i];
    }
    _sortDescs.clear();
}

void RankSortDescription::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_sortDescs);
    dataBuffer.write(_percent);
}

void RankSortDescription::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_sortDescs);
    dataBuffer.read(_percent);
}

void RankSortDescription::setSortDescs(const std::vector<SortDescription*> &sortDesc) {
    _sortDescs = sortDesc;
}

void RankSortDescription::setPercent(const std::string &percentStr) {
    _percent = autil::StringUtil::fromString<float>(percentStr);
}

std::string RankSortDescription::toString() const {
    std::string descStr;
    for (size_t i = 0; i < _sortDescs.size(); i++) {
        assert(_sortDescs[i]);
        descStr.append(_sortDescs[i]->toString());
        descStr.append("#");
    }
    descStr.append(",percent:");
    descStr.append(StringUtil::toString(_percent));
    return descStr;
}

END_HA3_NAMESPACE(common);

