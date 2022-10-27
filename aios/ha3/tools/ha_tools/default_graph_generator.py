# Python
import tensorflow as tf
from tensorflow.python.ops import control_flow_ops
from optparse import OptionParser
from argparse import ArgumentParser
from multiple_opdef_module import MultipleOpdefModule
import sys

class Ha3GraphGenerator(object):
    def __init__(self):
        self.parser = ArgumentParser()

    def usage(self):
        print self.parser.print_help()

    def addOptions(self):
        self.parser.add_argument('-i', '--input', dest='defSoList', nargs='*', default=[])
        self.parser.add_argument('-o', '--output', action='store', dest='outputName')
        self.parser.add_argument('-t', '--type', action='store', dest='roleType')
        self.parser.add_argument('-w', '--way-count', action='store', dest='wayCount')
        self.parser.add_argument('-b', '--batch-size', action='store', dest='batchSize')

    def parseParams(self):
        self.addOptions()
        options = self.parser.parse_args()
        self.options = options
        ret = self.checkOptionsValidity(options)
        if not ret:
            print "ERROR: checkOptionsValidity Failed!"
            return False
        self.initMember(options)
        return True

    def checkOptionsValidity(self, options):
        if not options.defSoList:
            print "ERROR: ops def so must be specified"
            return False
        if options.outputName == None or options.outputName == '':
            print "ERROR: output path must be specified"
            return False

        if options.roleType == None or (options.roleType != 'searcher' and
                                        options.roleType != 'qrs' and
                                        options.roleType != 'sql' and
                                        options.roleType != 'searcher_parallel' and
                                        options.roleType != 'aggWithWhile' and
                                        options.roleType != 'aggNoWhile' and
                                        options.roleType != 'summaryWithExtra' and
                                        options.roleType != 'searcher_aux_scan'):
            print "ERROR: role type must be searcher or qrs or summaryWithExtra"
            return False

        if options.roleType not in ['aggWithWhile', 'aggNoWhile', 'searcher_parallel'] and options.wayCount != None:
            print "ERROR: waycount must be specific with agg or searcher_parallel mode"
            return False

        if options.roleType != 'aggNoWhile' and options.batchSize != None:
            print "ERROR: batchsize must be specific with aggNoWhile mode"
            return False

        return True

    def initMember(self, options):
        self.defSoList = options.defSoList
        self.outputName = options.outputName
        self.roleType = options.roleType
        if options.wayCount != None:
            self.wayCount = int(options.wayCount)
        if options.batchSize != None:
            self.batchSize = int(options.batchSize)

    def getOutputName(self):
        return self.outputName

    def getGraph(self):
        if self.roleType == 'qrs':
            return self._genQrsGraph()
        elif self.roleType == 'searcher':
            return self._genSearcherGraph()
        elif self.roleType == 'searcher_parallel':
            return self._genSearcherParallelGraph()
        elif self.roleType == 'aggWithWhile':
            return self._genAggGraphWithWhile()
        elif self.roleType == 'aggNoWhile':
            return self._genAggGraphNoWhile()
        elif self.roleType == 'sql':
            return self._genSqlGraph()
        elif self.roleType == 'summaryWithExtra':
            return self._genSearcherGraph(summaryWithExtra = True)
        elif self.roleType == 'searcher_aux_scan':
            return self._genSearcherWithAuxScanGraph()

    def _genSqlGraph(self):
        g = tf.Graph()
        with g.as_default():
            turing_sql_module = MultipleOpdefModule(self.defSoList)
            sql_request = tf.placeholder(tf.string, name='sql_request')
            sql_result = turing_sql_module.run_sql_op(sql_request, name = "run_sql")
            with tf.control_dependencies([sql_result]):
                node_end = tf.no_op(name="sql_done")
        return g

    def _genAggGraphNoWhile(self):
        g = tf.Graph()
        way_count = self.wayCount
        batch_size = self.batchSize
        with g.as_default():
            turing_agg_module = MultipleOpdefModule(self.defSoList)
            input_request = tf.placeholder(tf.variant, name='ha3_request')
            request = turing_agg_module.request_init_op(input_request, name = 'init_request')
            layer_metas = turing_agg_module.layer_metas_create_op(request, name = 'create_range')
            aggres = []
            split_layer_metas = turing_agg_module.range_split_op(layer_metas, N = way_count,
                                                                 name = 'split_range')
            for layer_metas in split_layer_metas:
                expression_resource = turing_agg_module.prepare_expression_resource_op(
                    request, name = "resource_prepare")
                seek_iterator = turing_agg_module.seek_iterator_prepare_op(
                    layer_metas, expression_resource, name = "seek_prepare")
                with tf.control_dependencies([seek_iterator]):
                    aggregator = turing_agg_module.agg_prepare_op(
                        expression_resource, name = "agg_prepare",
                        relative_path="agg.conf",
                        json_path="aggregate_sampler_config")

                agg = turing_agg_module.seek_and_agg_op(
                    aggregator, seek_iterator, batch_size = batch_size, name = "seek_and_agg")
                aggres.append(agg)

            aggresult = turing_agg_module.agg_merge_op(aggres)
            ha3result = turing_agg_module.agg_result_construct_op(aggresult, name = "ha3_result")
            with tf.control_dependencies([ha3result]):
                node_end = tf.no_op(name="ha3_search_done")

        return g


    def _genAggGraphWithWhile(self):
        g = tf.Graph()
        way_count = self.wayCount
        def body(eof, seek_iterator, aggregator):
            output_iterator, matchdocs, seek_eof = turing_agg_module.seek_op(
                seek_iterator, batch_size = 10240, name = "seek_docs")
            output_aggregator, output_matchdocs = turing_agg_module.aggregator_op(
                aggregator, matchdocs, name = "agg_docs")
            eof_seek = turing_agg_module.match_doc_release_op(seek_eof, output_matchdocs,
                                                              name = "release_docs")
            return eof_seek, output_iterator, output_aggregator


        def condition(eof_seek, seek_iterator, aggregator):
            return tf.equal(False, eof_seek, name ="cond_equal")

        with g.as_default():
            turing_agg_module = MultipleOpdefModule(self.defSoList)
            input_request = tf.placeholder(tf.variant, name='ha3_request')
            request = turing_agg_module.request_init_op(input_request, name = 'init_request')
            layer_metas = turing_agg_module.layer_metas_create_op(request,
                                                                  name = 'create_layer_metas')

            split_layer_metas = turing_agg_module.range_split_op(layer_metas, N = way_count,
                                                                 name = 'split_ranges')
            aggres = []
            for layer_metas in split_layer_metas:
                expression_resource = turing_agg_module.prepare_expression_resource_op(
                    request, name = "prepare_expression_resource")
                seek_iterator = turing_agg_module.seek_iterator_prepare_op(
                        layer_metas, expression_resource, name = "prepare_seek_iterator")
                with tf.control_dependencies([seek_iterator]):
                    aggregator = turing_agg_module.agg_prepare_op(expression_resource,
                                                                  name = "agg_prepare",
                                                                  relative_path="agg.conf",
                                                                  json_path="aggregate_sampler_config")
                eof = tf.constant(False)
                #eof = tf.Variable(False, name="seek_end")
                ret_vals = control_flow_ops.while_loop(condition, body,
                                                       [eof, seek_iterator, aggregator],
                                               shape_invariants = [tf.TensorShape(None),
                                                                   tf.TensorShape(None),
                                                                   tf.TensorShape(None)],
                                               parallel_iterations = 1)
                aggres.append(ret_vals[2])

            aggresult = turing_agg_module.agg_merge_op(aggres)
            ha3result = turing_agg_module.agg_result_construct_op(aggresult, name = "ha3_result")
            with tf.control_dependencies([ha3result]):
                node6 = tf.no_op(name="ha3_search_done")

        return g

    def _genQrsGraph(self):
        g = tf.Graph()
        with g.as_default():
            fake_placeholder = tf.placeholder(tf.int32, name='fake_placeholder')
            fake_identity = tf.identity(fake_placeholder, name="fake_identity")

            turing_module = MultipleOpdefModule(self.defSoList)
            ha3_request = tf.placeholder(tf.variant, name='ha3_request')
            ha3_results = tf.placeholder(tf.variant, name='ha3_results')
            with_matchdoc, without_matchdoc = turing_module.ha3_result_merge_op(ha3_request, ha3_results, name='result_tmp')

            p_t = tf.identity(with_matchdoc)
            p_f = tf.identity(without_matchdoc)

            with tf.control_dependencies([p_t]):
                id_wm = tf.identity(p_t)
                ha3_split = turing_module.ha3_result_split_op(
                    ha3_request, id_wm, name='ha3_split')

                ha3_qrs_sorter = turing_module.ha3_sorter_op(
                    ha3_request, ha3_split[0], ha3_split[1], ha3_split[2],
                    ha3_split[3], ha3_split[5], name='ha3_qrs_sorter', attr_location='SL_QRS')

                reconstruct = turing_module.ha3_result_reconstruct_op(ha3_split[4], ha3_qrs_sorter[0],
                                                                      ha3_qrs_sorter[1], ha3_qrs_sorter[2],
                                                                      name='reconstruct')

            with tf.control_dependencies([p_f]):
                id_wom = tf.identity(p_f)

            ha3_result_tmp, port = control_flow_ops.merge([reconstruct, id_wom], name='ha3_result_merge')

            ha3_result = tf.identity(ha3_result_tmp, name="ha3_result")

            tf.identity(ha3_result, name="ha3_search_done")
        return g

    def _genSearcherGraph(self, summaryWithExtra = False):
        g = tf.Graph()
        with g.as_default():
            turing_module = MultipleOpdefModule(self.defSoList)
            request_not_init_yet = tf.placeholder(tf.variant, name='ha3_request')
            request_init = turing_module.ha3_request_init_op(request_not_init_yet, name='request_init')
            request = turing_module.add_degrade_info_op(request_init, name='add_degrade')
            isPhaseOne = turing_module.is_phase_one_op(request, name='is_phase_one')
            s_f, s_t = control_flow_ops.switch(isPhaseOne, isPhaseOne)
            p_f = tf.identity(s_f, name = 'switch_f')
            p_t = tf.identity(s_t, name = 'switch_t')
            with tf.control_dependencies([p_t]):
                seek_prepare = turing_module.ha3_searcher_prepare_op(request, name="seek_prepare")

            with tf.control_dependencies([seek_prepare]):
                cache_prepare = turing_module.ha3_cache_prepare_op(request, name="cache_prepare")

            kvpair = turing_module.ha3_request_to_kv_pair_op(request_init,
                                                             name = "ha3_request_to_kv_pair")
            code_compile = turing_module.cava_compile_op(kvpair,
                                                         cava_code_key = "cava_code",
                                                         name = "cava_query_code_compile")

            with tf.control_dependencies([cache_prepare, code_compile]):
                seek_res = turing_module.ha3_seek_op(request, name="seek")
            srcCount = tf.constant(1, dtype=tf.uint16)
            with tf.control_dependencies([kvpair, seek_res[0], code_compile]):
                final_score = turing_module.cava_scorer_op(kvpair, seek_res[0],
                                                           name="final_score",
                                                           enable_score_key="enable_final_score",
                                                           cava_scorer_name_key = "final_scorer_name",
                                                           score_ref_name = "_@_build_in_score")
            error_merge = turing_module.error_result_merge_op(final_score[1],
                                                              name='final_score_error_merge',
                                                              error_code=30030)
            with tf.control_dependencies([error_merge, final_score[0], seek_res[1],
                                          seek_res[2], seek_res[3], seek_res[4]]):
                sort_res = turing_module.ha3_sorter_op(request, final_score[0],
                                                       seek_res[1],seek_res[2],
                                                       seek_res[3], srcCount, attr_location="SL_SEARCHER" )
            release_redundant_res = turing_module.ha3_release_redundant_v2_op(request, sort_res[0],
                                                                              name="release_before_fill_attributes")
            fillattr_res = turing_module.ha3_fill_attributes_op(request, release_redundant_res);

            searchResult = turing_module.ha3_result_construct_op(request, fillattr_res,
                                                         sort_res[1], sort_res[2],
                                                         seek_res[4], name="phase1_result")

            no_cache, miss_cache, hit_cache = turing_module.ha3_searcher_cache_switch_op(searchResult, name="cache_switch")
            miss_cache_result = turing_module.ha3_searcher_put_cache_op(request, miss_cache, name="put_cache")

            cache_and_inc_result = turing_module.ha3_cache_refresh_attr_op(request, hit_cache, name="refresh_attr")

            hit_with_matchdoc, hit_without_matchdoc = turing_module.ha3_result_merge_op(
                request, cache_and_inc_result, name='merge_cache_and_inc')

            cache_result_split = turing_module.ha3_result_split_op(
                request, hit_with_matchdoc, name='cache_result_split')


            ha3_cache_sorter = turing_module.ha3_sorter_op(
                request, cache_result_split[0], cache_result_split[1], cache_result_split[2],
                cache_result_split[3], cache_result_split[5], name='ha3_cache_sorter', attr_location='SL_SEARCHCACHEMERGER')

            result_after_cachesorter = turing_module.ha3_result_reconstruct_op(cache_result_split[4], ha3_cache_sorter[0],
                                                                               ha3_cache_sorter[1], ha3_cache_sorter[2],
                                                                               name='reconstruct_after_cachesorter')

            result_before_redundant, portId = control_flow_ops.merge([
                no_cache, miss_cache_result, result_after_cachesorter, hit_without_matchdoc])

            result_before_fill_id = turing_module.ha3_release_redundant_op(result_before_redundant, name='release_redundant')

            searchPhaseOneResult = turing_module.ha3_fill_global_id_info_op(request, result_before_fill_id, name='fill_global_id')

            with tf.control_dependencies([p_f]):
                searchPhaseTwoResultTmp = turing_module.ha3_summary_op(request, name="phase2_result", force_allow_lack_of_summary = summaryWithExtra)

            if summaryWithExtra:
                searchPhaseTwoResult = turing_module.ha3_extra_summary_op(request, searchPhaseTwoResultTmp, name="extra_phase2_result")
            else:
                searchPhaseTwoResult = searchPhaseTwoResultTmp

            finalResult, portId = control_flow_ops.merge([seek_res[5], searchPhaseOneResult, searchPhaseTwoResult], name="ha3_result_merge");

            finalResultOut = turing_module.ha3_end_identity_op(finalResult, name="ha3_result")

            with tf.control_dependencies([finalResultOut]):
                node6 = tf.no_op(name="ha3_search_done")
        return g

    def _genSearcherParallelGraph(self, summaryWithExtra = False):
        graph = tf.Graph()
        way_count = self.wayCount
        with graph.as_default():
            turing_module = MultipleOpdefModule(self.defSoList)
            request_not_init_yet = tf.placeholder(tf.variant, name='ha3_request')
            request_init = turing_module.ha3_request_init_op(request_not_init_yet, name='request_init')
            request = turing_module.add_degrade_info_op(request_init, name='add_degrade')
            isPhaseOne = turing_module.is_phase_one_op(request, name='is_phase_one')
            s_f, s_t = control_flow_ops.switch(isPhaseOne, isPhaseOne)
            p_f = tf.identity(s_f, name = 'switch_f')
            p_t = tf.identity(s_t, name = 'switch_t')
            with tf.control_dependencies([p_t]):
                seek_split_layermetas= turing_module.ha3_seek_para_split_op(request, N = way_count, name="seek_split")
            para_resources = []
            para_results = []
            has_errors = []
            for layermetas in seek_split_layermetas:
                seek_request, seek_para_resource = turing_module.ha3_searcher_prepare_para_op(request, layermetas, name="seek_prepare_para")
                para_resources.append(seek_para_resource)
                seek_para_result, has_error = turing_module.ha3_seek_para_op(seek_request, seek_para_resource, name="seek")
                para_results.append(seek_para_result)
                has_errors.append(has_error)
            seek_res = turing_module.ha3_seek_para_merge_op(request, para_resources, para_results, has_errors)
            srcCount = tf.constant(1, dtype=tf.uint16)
            with tf.control_dependencies([seek_res[0], seek_res[1], seek_res[2], seek_res[3], seek_res[4]]):
                sort_res = turing_module.ha3_sorter_op(request,seek_res[0],
                                                       seek_res[1],seek_res[2],
                                                       seek_res[3], srcCount, attr_location="SL_SEARCHER" )
            fillattr_res = turing_module.ha3_fill_attributes_op(request, sort_res[0]);

            result_before_redundant = turing_module.ha3_result_construct_op(request, fillattr_res,
                                                         sort_res[1], sort_res[2],
                                                         seek_res[4], name="phase1_result")

            result_before_fill_id = turing_module.ha3_release_redundant_op(result_before_redundant, name='release_redundant')

            searchPhaseOneResult = turing_module.ha3_fill_global_id_info_op(request, result_before_fill_id, name='fill_global_id')

            with tf.control_dependencies([p_f]):
                searchPhaseTwoResultTmp = turing_module.ha3_summary_op(request, name="phase2_result", force_allow_lack_of_summary = summaryWithExtra)

            if summaryWithExtra:
                searchPhaseTwoResult = turing_module.ha3_extra_summary_op(request, searchPhaseTwoResultTmp, name="extra_phase2_result")
            else:
                searchPhaseTwoResult = searchPhaseTwoResultTmp

            finalResult, portId = control_flow_ops.merge([seek_res[5], searchPhaseOneResult, searchPhaseTwoResult], name="ha3_result_merge");

            finalResultOut = turing_module.ha3_end_identity_op(finalResult, name="ha3_result")

            with tf.control_dependencies([finalResultOut]):
                node6 = tf.no_op(name="ha3_search_done")
        return graph

    def _genSearcherWithAuxScanGraph(self, summaryWithExtra = False):
        g = tf.Graph()
        with g.as_default():
            turing_module = MultipleOpdefModule(self.defSoList)
            request_not_init_yet = tf.placeholder(tf.variant, name='ha3_request')
            request_init = turing_module.ha3_request_init_op(request_not_init_yet, name='request_init')
            request = turing_module.add_degrade_info_op(request_init, name='add_degrade')
            isPhaseOne = turing_module.is_phase_one_op(request, name='is_phase_one')
            s_f, s_t = control_flow_ops.switch(isPhaseOne, isPhaseOne)
            p_f = tf.identity(s_f, name = 'switch_f')
            p_t = tf.identity(s_t, name = 'switch_t')

            with tf.control_dependencies([p_t]):
                seek_prepare = turing_module.ha3_searcher_prepare_op(request, name="seek_prepare")
                aux_scan_res = turing_module.ha3_aux_scan_op(request, name="aux_scan")

            with tf.control_dependencies([seek_prepare]):
                cache_prepare = turing_module.ha3_cache_prepare_op(request, name="cache_prepare")

            with tf.control_dependencies([cache_prepare]):
                seek_res = turing_module.ha3_seek_and_join_op(request, aux_scan_res, name="seek_and_join")

            srcCount = tf.constant(1, dtype=tf.uint16)
            with tf.control_dependencies([seek_res[0], seek_res[1], seek_res[2], seek_res[3], seek_res[4]]):
                sort_res = turing_module.ha3_sorter_op(request,seek_res[0],
                                                       seek_res[1],seek_res[2],
                                                       seek_res[3], srcCount, attr_location="SL_SEARCHER" )
            release_redundant_res = turing_module.ha3_release_redundant_v2_op(request, sort_res[0],
                                                                              name="release_before_fill_attributes")

            fillattr_res = turing_module.ha3_fill_attributes_op(request, release_redundant_res);

            searchResult = turing_module.ha3_result_construct_op(request, fillattr_res,
                                                         sort_res[1], sort_res[2],
                                                         seek_res[4], name="phase1_result")

            no_cache, miss_cache, hit_cache = turing_module.ha3_searcher_cache_switch_op(searchResult, name="cache_switch")
            miss_cache_result = turing_module.ha3_searcher_put_cache_op(request, miss_cache, name="put_cache")

            cache_and_inc_result = turing_module.ha3_cache_refresh_attr_op(request, hit_cache, name="refresh_attr")

            hit_with_matchdoc, hit_without_matchdoc = turing_module.ha3_result_merge_op(
                request, cache_and_inc_result, name='merge_cache_and_inc')

            cache_result_split = turing_module.ha3_result_split_op(
                request, hit_with_matchdoc, name='cache_result_split')

            ha3_cache_sorter = turing_module.ha3_sorter_op(
                request, cache_result_split[0], cache_result_split[1], cache_result_split[2],
                cache_result_split[3], cache_result_split[5], name='ha3_cache_sorter', attr_location='SL_SEARCHCACHEMERGER')

            result_after_cachesorter = turing_module.ha3_result_reconstruct_op(cache_result_split[4], ha3_cache_sorter[0],
                                                                               ha3_cache_sorter[1], ha3_cache_sorter[2],
                                                                               name='reconstruct_after_cachesorter')

            result_before_redundant, portId = control_flow_ops.merge([
                no_cache, miss_cache_result, result_after_cachesorter, hit_without_matchdoc])

            result_before_fill_id = turing_module.ha3_release_redundant_op(result_before_redundant, name='release_redundant')

            searchPhaseOneResult = turing_module.ha3_fill_global_id_info_op(request, result_before_fill_id, name='fill_global_id')

            with tf.control_dependencies([p_f]):
                searchPhaseTwoResultTmp = turing_module.ha3_summary_op(request, name="phase2_result", force_allow_lack_of_summary = summaryWithExtra)

            if summaryWithExtra:
                searchPhaseTwoResult = turing_module.ha3_extra_summary_op(request, searchPhaseTwoResultTmp, name="extra_phase2_result")
            else:
                searchPhaseTwoResult = searchPhaseTwoResultTmp

            finalResult, portId = control_flow_ops.merge([seek_res[5], searchPhaseOneResult, searchPhaseTwoResult], name="ha3_result_merge");

            finalResultOut = turing_module.ha3_end_identity_op(finalResult, name="ha3_result")

            with tf.control_dependencies([finalResultOut]):
                node6 = tf.no_op(name="ha3_search_done")
        return g

if __name__ == '__main__':
    graphGenerator = Ha3GraphGenerator()

    if not graphGenerator.parseParams():
        graphGenerator.usage()
        sys.exit(-1)

    searcherGraph = graphGenerator.getGraph()
    g_def = searcherGraph.as_graph_def();
    outputName = graphGenerator.getOutputName()
    with open("./%s.def" % outputName, 'w') as f:
        f.write(g_def.SerializeToString())
    with open("./%s.pbtxt" % outputName, 'w') as f:
        f.write(str(g_def))

    writer = tf.summary.FileWriter("./%s.tensorboard" % outputName, searcherGraph)
