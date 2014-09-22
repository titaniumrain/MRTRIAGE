import sim_funcs



def get_dist_func(feature_name):
    if feature_name == 'file_name' or feature_name == 'from_address' or feature_name == 'av_avira' or feature_name == 'host':
        return getattr(sim_funcs, 'sim_Ngram')
    elif feature_name == 'subject':
        return getattr(sim_funcs, 'sim_levenshtein')
    elif feature_name == 'IP' or feature_name == 'source_ip':
        return getattr(sim_funcs, 'sim_IP')
    elif feature_name == 'date' or feature_name == 'day':
        return getattr(sim_funcs, 'sim_date')
    elif feature_name == 'file_ssdeep':
        return getattr(sim_funcs, 'sim_ssdeep')
    elif feature_name == 'nw_connect' or feature_name == 'uri_domain':
        return getattr(sim_funcs, 'sim_tanimoto')
    else:
        return getattr(sim_funcs, 'sim_generic')