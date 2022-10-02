from inspect import getargs
import multiprocessing as mp 
from multiprocessing import Pool



def call_method(cls, name):
    return getattr(cls, name)()


def mp_apply_async(func, argument_lst, num_progress, call_back = None, err_call_back = None):
    pool = Pool(num_progress)
    result_lst = []
    missions = [
        pool.apply_async(
            func=func, args=(*argument,), callback=call_back, error_callback=err_call_back 
            ) if isinstance(argument, tuple) else pool.apply_async(func=func, args=(*argument,), callback=call_back, error_callback=err_call_back) \
            for argument in argument_lst
    ]
    pool.close()
    pool.join()
    for job in missions:
        result_lst.append(job.get())
    
    return result_lst
