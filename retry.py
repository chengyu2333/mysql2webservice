def retry(attempt):
    def decorator(func):
        def wrapper(*args, **kw):
            att = 0
            while att < attempt:
                try:
                    print(att)
                    return func(*args, **kw)
                except Exception as e:
                    att += 1
                    if att == attempt:
                        raise
                    else:
                        print("retry")
        return wrapper
    return decorator