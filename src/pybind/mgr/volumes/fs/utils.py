from errno import *
from uuid import UUID
from os.path import join
from logging import getLogger

from .exception import VolumeException, InvalidUuidError


log = getLogger(__name__)


def ensure_uuid_is_valid(uuid):
    '''
    If UUID is invaid, raise InvalidUuidException.
    '''
    uuid_type = type(uuid)
    if uuid_type is str:
        pass
    elif uuid_type is bytes:
        uuid = uuid.decode('utf-8')
    else:
        raise VolumeException(EINVAL,
                              ('received invalid type for uuid, expected str '
                               f'or bytes. uuid_type={uuid_type} uuid={uuid}'))

    try:
        UUID(uuid, version=4)
    except Exception as e:
        raise InvalidUuidException(EINVAL,
                                   (f'received invalid uuid. uuid = {uuid}. '
                                    f'exception raised by uuid module: {e}'))


def safe_join(*args):
    '''
    Convert members of args to bytes before passing them to os.path.join() and
    and return its return value. Excepatable types: str, int, float, bool.

    :rtype: bytes
    '''
    newargs = to_bytes(*args)
    for index, var in enumerate(newargs):
        if index > 1 and var[0] == '/':
            raise VolumeException(EINVAL,
                                  ('safe_join() received non-first arg starting '
                                   'with "/"'))

    return join(*newargs)


def to_bytes(*args):
    '''
    Convert all of args to bytes. Valid types: str, bytes, int, float and bool.

    :rtype: bytes or list of bytes
    '''
    args_type = type(args)
    if args_type is str:
        return args.encode('utf-8')
    if args_type is bytes:
        return args
    if args_type in (int, float, bool):
        return str(args)
    elif args_type in (list, tuple):
        pass
    else:
        raise VolumeException(EINVAL,
                              ('received invalid type. args_type = '
                               f'{args_type} args = {args}'))

    newargs = []
    for var in args:
        var_type = type(var)
        if var_type is bytes:
            newargs.append(var)
        elif var_type is str:
            newargs.append(var.encode('utf-8'))
        elif var_type in (int, float, bool):
            newargs.append(str(var).encode('utf-8'))
        else:
            raise VolumeException(EINVAL,
                                  ('received invalid type. expected types: str, '
                                   'bytes, int, float or bool, received type: '
                                   f'{var_type} var = {var}'))
    return newargs


def to_str(*args):
    '''
    Convert all of args to str. Valid types: str, bytes, int, float and bool.

    :rtype: str or list of str
    '''
    args_type = type(args)
    if args_type is str:
        return args
    if args_type is bytes:
        return args.decode('utf-8')
    if args_type in (int, float, bool):
        return str(args)
    elif args_type in (list, tuple):
        pass
    else:
        raise VolumeException(EINVAL,
                              f'invalid type. args_type = {args_type} '
                               f'args = {args}')

    newargs = []
    for var in args:
        var_type = type(var)

        if var_type is str:
            newargs.append(var)
        elif var_type is bytes:
            newargs.append(var.decode('utf-8'))
        elif var_type in (int, float, bool):
            newargs.append(str(var))
        else:
            raise VolumeException(EINVAL,
                                  'invalid type. var_type = {var_type} '
                                  f'var = {var} args = {args}')
    return newargs
