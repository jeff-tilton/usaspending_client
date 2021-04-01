from functools import wraps


def log_decorator(logger, level=10):
    def real_decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            name = function.__name__
            logger.log(level=level, msg=f"Start {name}")
            out = function(*args, **kwargs)
            logger.log(level=level, msg=f"End {name}")
            return out

        return wrapper

    return real_decorator


# TODO: make this do a hierarchical index
def flatten_dict(nested_dictionary, delimiter=":"):
    "Flattens a nested dictionary"
    res = {}

    def get_all_values(nested_dictionary, parent_key, delimiter=delimiter):
        # ref: https://www.kite.com/python/answers/how-to-loop-through-all-nested-dictionary-values-using-a-for-loop-in-python
        for key, value in nested_dictionary.items():
            if type(value) is dict:
                if parent_key:
                    k = f"{parent_key}{delimiter}{key}"
                else:
                    k = key
                get_all_values(value, parent_key=k)
            else:
                k = f"{parent_key}{delimiter}{key}"
                res.update({k: value})

    get_all_values(nested_dictionary=nested_dictionary, parent_key="")
    return res
