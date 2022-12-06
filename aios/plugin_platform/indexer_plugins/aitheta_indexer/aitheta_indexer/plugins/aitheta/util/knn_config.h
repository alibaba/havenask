/**
 * @file   knn_dynamic_config.h
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 17:16:35 2019
 *
 * @brief
 *
 *
 */

#ifndef __KNN_DYNAMIC_CONFIG_H_
#define __KNN_DYNAMIC_CONFIG_H_

#include <string>
#include <vector>
#include <map>
#include <autil/legacy/jsonizable.h>
#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/util/key_value_map.h>
#include <indexlib/file_system/directory.h>

IE_NAMESPACE_BEGIN(aitheta_plugin);

class KnnRangeParams : public autil::legacy::Jsonizable {
 public:
    KnnRangeParams();
    ~KnnRangeParams();

 public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

    bool IsAvailable() { return upperLimit > 0; }
    bool operator<(const KnnRangeParams &other) const { return upperLimit < other.upperLimit; }
    void Merge(const KnnRangeParams &otherParams);

 public:
    int32_t upperLimit;
    util::KeyValueMap params;

 private:
    IE_LOG_DECLARE();
};

class KnnStrategy : public autil::legacy::Jsonizable {
 public:
    KnnStrategy();
    ~KnnStrategy();

 public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool IsAvailable();
    void setName(const std::string &strategyName) { name = strategyName; }

 public:
    std::string name;
    bool useAsDefault;
    std::vector<KnnRangeParams> rangeParamsList;  // TODO(luoli.hn) 改成RangeParams

 private:
    IE_LOG_DECLARE();
};

class KnnStrategies : public autil::legacy::Jsonizable {
 public:
    KnnStrategies();
    ~KnnStrategies();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool IsAvailable();

 public:
    std::string type;
    std::map<std::string, KnnStrategy> strategy2RangeParams;

 private:
    IE_LOG_DECLARE();
};

class KnnConfig : public autil::legacy::Jsonizable {
 public:
    KnnConfig();
    ~KnnConfig();

 public:
    bool Load(const std::string& path);
    bool Load(const IE_NAMESPACE(file_system)::DirectoryPtr &directory);
    bool Dump(const IE_NAMESPACE(file_system)::DirectoryPtr &directory) const;
    bool IsAvailable() const;

 public:
    std::map<std::string, KnnStrategies> type2Strategies;

 private:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

 private:
    IE_LOG_DECLARE();
};

// TODO(Compatible for ha3, delete it in later version)
static const std::string kDefaultKnnConfig = R"({
    "dynamic_configs": [
        {
            "type": "hc",
            "strategies": [
                {
                    "name": "v0",
                    "dynamic_param_list": [
                        {
                            "upper_limit": 50000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "10",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "10",
                                "proxima.hc.searcher.max_scan_num": "10000"
                            }
                        },
                        {
                            "upper_limit": 100000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "20",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "20",
                                "proxima.hc.searcher.max_scan_num": "10000"
                            }
                        },
                        {
                            "upper_limit": 150000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "30",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "30",
                                "proxima.hc.searcher.max_scan_num": "10000"
                            }
                        },
                        {
                            "upper_limit": 200000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "40",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "40",
                                "proxima.hc.searcher.max_scan_num": "10000"
                            }
                        },
                        {
                            "upper_limit": 250000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "50",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "50",
                                "proxima.hc.searcher.max_scan_num": "10000"
                            }
                        },
                        {
                            "upper_limit": 300000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "60",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "60",
                                "proxima.hc.searcher.max_scan_num": "20000"
                            }
                        },
                        {
                            "upper_limit": 400000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "80",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "60",
                                "proxima.hc.searcher.max_scan_num": "20000"
                            }
                        },
                        {
                            "upper_limit": 500000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "60",
                                "proxima.hc.searcher.max_scan_num": "20000"
                            }
                        },
                        {
                            "upper_limit": 1000000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "400",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "60",
                                "proxima.hc.searcher.max_scan_num": "50000"
                            }
                        },
                        {
                            "upper_limit": 2000000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "1000",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "60",
                                "proxima.hc.searcher.max_scan_num": "50000"
                            }
                        },
                        {
                            "upper_limit": 2147483647,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "1000",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "60",
                                "proxima.hc.searcher.max_scan_num": "50000"
                            }
                        }
                    ]
                },
                {
                    "name": "mainse",
                    "as_default": true,
                    "dynamic_param_list": [
                        {
                            "upper_limit": 50000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "10",
                                "proxima.hc.searcher.scan_num_in_level_1": "100",
                                "proxima.hc.searcher.scan_num_in_level_2": "1000",
                                "proxima.hc.searcher.max_scan_num": "25000"
                            }
                        },
                        {
                            "upper_limit": 100000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "20",
                                "proxima.hc.searcher.scan_num_in_level_1": "100",
                                "proxima.hc.searcher.scan_num_in_level_2": "2000",
                                "proxima.hc.searcher.max_scan_num": "25000"
                            }
                        },
                        {
                            "upper_limit": 200000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "30",
                                "proxima.hc.searcher.scan_num_in_level_1": "100",
                                "proxima.hc.searcher.scan_num_in_level_2": "3000",
                                "proxima.hc.searcher.max_scan_num": "25000"
                            }
                        },
                        {
                            "upper_limit": 300000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "40",
                                "proxima.hc.searcher.scan_num_in_level_1": "100",
                                "proxima.hc.searcher.scan_num_in_level_2": "4000",
                                "proxima.hc.searcher.max_scan_num": "25000"
                            }
                        },
                        {
                            "upper_limit": 500000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "50",
                                "proxima.hc.searcher.scan_num_in_level_1": "100",
                                "proxima.hc.searcher.scan_num_in_level_2": "5000",
                                "proxima.hc.searcher.max_scan_num": "35000"
                            }
                        },
                        {
                            "upper_limit": 1000000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "80",
                                "proxima.hc.searcher.scan_num_in_level_2": "8000",
                                "proxima.hc.searcher.max_scan_num": "35000"
                            }
                        },
                        {
                            "upper_limit": 2000000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "500",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "80",
                                "proxima.hc.searcher.scan_num_in_level_2": "8000",
                                "proxima.hc.searcher.max_scan_num": "35000"
                            }
                        },
                        {
                            "upper_limit": 2147483647,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "500",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "80",
                                "proxima.hc.searcher.scan_num_in_level_2": "8000",
                                "proxima.hc.searcher.max_scan_num": "35000"
                            }
                        }
                    ]
                },
                {
                    "name": "old_default",
                    "dynamic_param_list": [
                        {
                            "upper_limit": 50000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "10",
                                "proxima.hc.searcher.scan_num_in_level_1": "50",
                                "proxima.hc.searcher.max_scan_num": "1000"
                            }
                        },
                        {
                            "upper_limit": 100000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "20",
                                "proxima.hc.searcher.scan_num_in_level_1": "50",
                                "proxima.hc.searcher.max_scan_num": "1000"
                            }
                        },
                        {
                            "upper_limit": 200000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "30",
                                "proxima.hc.searcher.scan_num_in_level_1": "50",
                                "proxima.hc.searcher.max_scan_num": "1000"
                            }
                        },
                        {
                            "upper_limit": 300000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "40",
                                "proxima.hc.searcher.scan_num_in_level_1": "30",
                                "proxima.hc.searcher.max_scan_num": "1000"
                            }
                        },
                        {
                            "upper_limit": 500000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "50",
                                "proxima.hc.searcher.scan_num_in_level_1": "30",
                                "proxima.hc.searcher.max_scan_num": "4000"
                            }
                        },
                        {
                            "upper_limit": 1000000,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "30",
                                "proxima.hc.searcher.max_scan_num": "4000"
                            }
                        },
                        {
                            "upper_limit": 2147483647,
                            "params": {
                                "proxima.hc.builder.num_in_level_1": "100",
                                "proxima.hc.builder.num_in_level_2": "100",
                                "proxima.hc.searcher.scan_num_in_level_1": "30",
                                "proxima.hc.searcher.max_scan_num": "4000"
                            }
                        }
                    ]
                }
            ]
        },
        {
            "type": "pq",
            "strategies": [
                {
                    "name": "default",
                    "dynamic_param_list": [
                        {
                            "upper_limit": 50000,
                            "params": {
                                "proxima.pq.builder.train_product_centroid_number": "256",
                                "proxima.pq.builder.train_coarse_centroid_number": "8192",
                                "proxima.pq.searcher.coarse_scan_ratio": "1",
                                "proxima.pq.searcher.product_scan_number": "5000"
                            }
                        },
                        {
                            "upper_limit": 100000,
                            "params": {
                                "proxima.pq.builder.train_product_centroid_number": "256",
                                "proxima.pq.builder.train_coarse_centroid_number": "8192",
                                "proxima.pq.searcher.coarse_scan_ratio": "1",
                                "proxima.pq.searcher.product_scan_number": "5000"
                            }
                        },
                        {
                            "upper_limit": 200000,
                            "params": {
                                "proxima.pq.builder.train_product_centroid_number": "256",
                                "proxima.pq.builder.train_coarse_centroid_number": "8192",
                                "proxima.pq.searcher.coarse_scan_ratio": "1",
                                "proxima.pq.searcher.product_scan_number": "5000"
                            }
                        },
                        {
                            "upper_limit": 300000,
                            "params": {
                                "proxima.pq.builder.train_product_centroid_number": "256",
                                "proxima.pq.builder.train_coarse_centroid_number": "8192",
                                "proxima.pq.searcher.coarse_scan_ratio": "1",
                                "proxima.pq.searcher.product_scan_number": "5000"
                            }
                        },
                        {
                            "upper_limit": 500000,
                            "params": {
                                "proxima.pq.builder.train_product_centroid_number": "256",
                                "proxima.pq.builder.train_coarse_centroid_number": "8192",
                                "proxima.pq.searcher.coarse_scan_ratio": "1",
                                "proxima.pq.searcher.product_scan_number": "5000"
                            }
                        },
                        {
                            "upper_limit": 1000000,
                            "params": {
                                "proxima.pq.builder.train_product_centroid_number": "256",
                                "proxima.pq.builder.train_coarse_centroid_number": "8192",
                                "proxima.pq.searcher.coarse_scan_ratio": "1",
                                "proxima.pq.searcher.product_scan_number": "5000"
                            }
                        },
                        {
                            "upper_limit": 2000000,
                            "params": {
                                "proxima.pq.builder.train_product_centroid_number": "256",
                                "proxima.pq.builder.train_coarse_centroid_number": "8192",
                                "proxima.pq.searcher.coarse_scan_ratio": "1",
                                "proxima.pq.searcher.product_scan_number": "5000"
                            }
                        },
                        {
                            "upper_limit": 2147483647,
                            "params": {
                                "proxima.pq.builder.train_product_centroid_number": "256",
                                "proxima.pq.builder.train_coarse_centroid_number": "8192",
                                "proxima.pq.searcher.coarse_scan_ratio": "1",
                                "proxima.pq.searcher.product_scan_number": "5000"
                            }
                        }
                    ]
                }
            ]
        }
    ]
})";

DEFINE_SHARED_PTR(KnnConfig);

IE_NAMESPACE_END(aitheta_plugin);
#endif  // __KNN_DYNAMIC_CONFIG_H_
