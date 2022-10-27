analyze log result: ./resualt_analyzer.awk <local_search_log_file>
extract query terms from log : ./term_extractor.awk <local_search_log_file> <terms_list_file>
sort terms by occurrence times : ./sort_term_by_freq.awk <terms_list_file> >> <term_list_sort_by_freq>
sort terms by df and total tf: ./sort_term_by_docs.awk <terms_list_file> >> <term_list_sort_by_docs>

<term_list_file> format: 
  term df total_tf

<term_list_sort_by_freq> format: 
  term df total_tf occ_times //term is distinct, occ_times is in descending order

<term_list_sort_by_docs> format: 
  term df total_tf occ_times //term is distinct, df is in descending order
