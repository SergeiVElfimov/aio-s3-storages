class StorageError(Exception):
    pass


class ObjectDoesNotExistError(StorageError):
    pass
